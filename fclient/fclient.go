/**
 * @brief		Engine Of Client
 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/cookiejar"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

type TaskStatusType int

const (
	ST_Actived      TaskStatusType = iota // Task Status Value = 0
	ST_Initializing                       // Task Status Value = 1
	ST_Completed                          // Task Status Value = 2
	ST_Ignore                             // Task Status Value = 3
	ST_Error                              // Task Status Value = 4
)

var (
	globalCurrentCookies   []*http.Cookie // Current Cookie
	globalCurrentCookieJar *cookiejar.Jar // Current CookieJar
)

// Package Initialization
func init() {
	globalCurrentCookies = nil
	globalCurrentCookieJar, _ = cookiejar.New(nil)
}

///////////////////////////////////// Resource Table Structure
// check xml result in response
type ResDownload struct {
	XMLName xml.Name `xml:"download"`
	URI     string   `xml:"uri,attr"`
	MD5     string   `xml:"md5,attr"`
	UPDATE  string   `xml:"update,attr"`
}

type ResourceList struct {
	XMLName  xml.Name      `xml:"resource"`
	Download []ResDownload `xml:"download"`
}

///////////////////////////////////// HTTP Client Engine Stucture/Class
type FileSyncClient struct {
	ServerHost    string              // Server IP + Port
	Account       string              // Server Login Username
	Password      string              // Server Login Password
	TTL           int                 // Time To Live
	objChannel    chan DownloadStatus // Channel Of Download Task
	ProgressFile  string              // Progress File Path
	TaskCount     int                 // Task Count
	CompleteCount int                 // Task Complete Count
}

///////////////////////////////////// [OutterMethod]
//  Active HTTP Client
func (pSelf *FileSyncClient) DoTasks(sTargetFolder string) {
	log.Println("[INF] FileSyncClient.DoTasks() : Executing Tasks ...... ")
	// Variable Definition
	var objResourceList ResourceList       // uri list object
	var objMapTask = make(map[string]bool) // map object [URI]true:sucess?false:failure

	// login
	if false == pSelf.login2Server() {
		log.Println("[ERR] FileSyncClient.DoTasks() : logon failure : invalid accountid or password, u r not allowed 2 logon the server.")
		return
	}

	// list resource table
	if false == pSelf.fetchResList(&objResourceList) {
		log.Println("[ERR] FileSyncClient.DoTasks() : cannot list resource table from server.")
		return
	}

	// downloading resources
	pSelf.objChannel = make(chan DownloadStatus)
	defer close(pSelf.objChannel) // defer this operation 2 release the channel object.
	for _, objRes := range objResourceList.Download {
		objMapTask[objRes.URI] = false
		go pSelf.fetchResource(objRes.URI, objRes.MD5, objRes.UPDATE, sTargetFolder)
	}

	///////////////////////////// Check Tasks Status //////////////////////////////
	pSelf.CompleteCount = 0
	pSelf.TaskCount = len(objMapTask)
	log.Println("[INF] FileSyncClient.DoTasks() : Task Number = ", pSelf.TaskCount)
	for i := 0; i < pSelf.TTL; i++ {
		select {
		case objStatus := <-pSelf.objChannel:
			if _, ok := objMapTask[objStatus.URI]; ok {
				if objStatus.Status == ST_Completed || objStatus.Status == ST_Ignore {
					objMapTask[objStatus.URI] = true // mark up: task completed
					if objStatus.Status == ST_Completed {
						log.Println("[INF] FileSyncClient.DoTasks() : [Complete] -->", objStatus.URI)
						pSelf.dumpProgress(1)
					} else if objStatus.Status == ST_Ignore {
						log.Println("[INF] FileSyncClient.DoTasks() : [Ignored] -->", objStatus.URI)
						pSelf.dumpProgress(1)
					} else if objStatus.Status == ST_Error {
						log.Println("[ERR] FileSyncClient.DoTasks() : [Exception] -->", objStatus.URI)
						os.Remove(objStatus.LocalPath)
					}

					count := 0
					for _, v := range objMapTask {
						if v == true {
							count++
						}
					}
					if count == len(objMapTask) {
						i = pSelf.TTL + 1
					}
				} else if objStatus.Status == ST_Error {
					objMapTask[objStatus.URI] = false // mark up: task failed
					log.Println("[WARN] FileSyncClient.DoTasks() : an error occur in task -->", objStatus.URI)
				}
			} else {
				log.Println("[WARN] FileSyncClient.DoTasks() : invalid URI -->", objStatus.URI)
			}
		default:
			time.Sleep(1 * time.Second)
		}
	}

	pSelf.dumpProgress(0)

	log.Println("[INF] FileSyncClient.DoTasks() : Mission Completed ...... ")
}

///////////////////////////////////// [InnerMethod]
// [method] download resource
type DownloadStatus struct {
	URI       string         // download url
	Status    TaskStatusType // task status
	LocalPath string         // File Path In Disk
}

func (pSelf *FileSyncClient) fetchResource(sUri, sMD5, sDateTime, sTargetFolder string) {
	var sLocalPath string = ""
	var nTaskStatus TaskStatusType = ST_Error // Mission Terminated!
	var objFCompare FComparison = FComparison{URI: sUri, MD5: sMD5, DateTime: sDateTime}
	var objUnzip Uncompress = Uncompress{TargetFolder: sTargetFolder}

	defer func() {
		if ST_Completed == nTaskStatus {
			if false == objUnzip.Unzip(sLocalPath, sUri) {
				nTaskStatus = ST_Error
			}
		}

		pSelf.objChannel <- DownloadStatus{URI: sUri, Status: nTaskStatus, LocalPath: sLocalPath} // Mission Finished!
	}()

	if true == objFCompare.Compare() {
		nTaskStatus = ST_Ignore // Mission Ignored!
	} else {
		// generate list Url string
		var sUrl string = fmt.Sprintf("http://%s/get?uri=%s", pSelf.ServerHost, sUri)
		log.Println("[INF] FileSyncClient.fetchResource() : [Downloading] -->", sUri, sMD5, sDateTime)

		// parse && read response string
		httpClient := http.Client{CheckRedirect: nil, Jar: globalCurrentCookieJar}
		httpReq, err := http.NewRequest("GET", sUrl, nil)
		httpRes, err := httpClient.Do(httpReq)
		if err != nil {
			log.Println("[ERR] FileSyncClient.fetchResource() :  error in response : ", sUrl, err.Error())
			return
		}

		// get absolute file path
		defer httpRes.Body.Close()
		sLocalFolder, err := filepath.Abs((filepath.Dir("./")))
		if err != nil {
			log.Println("[WARN] FileSyncClient.fetchResource() : failed 2 fetch absolute path of program")
			return
		}

		// save resource file 3 disk
		sLocalFolder = filepath.Join(sLocalFolder, CacheFolder)
		sLocalFile := filepath.Join(sLocalFolder, sUri)
		sMkFolder := path.Dir(sLocalFile)
		if "windows" == runtime.GOOS {
			sMkFolder = sLocalFile[:strings.LastIndex(sLocalFile, "\\")]
		}
		err = os.MkdirAll(sMkFolder, 0711)
		if err != nil {
			log.Printf("[WARN] FileSyncClient.fetchResource() : failed 2 create folder : %s : %s", sLocalFile, err.Error())
			return
		}

		objDataBuf := &bytes.Buffer{}
		_, err2 := objDataBuf.ReadFrom(httpRes.Body)
		if err2 != nil {
			log.Println("[ERR] FileSyncClient.fetchResource() :  cannot read response : ", sUrl, err.Error())
			return
		}

		objFile, _ := os.Create(sLocalFile)
		defer objFile.Close()
		_, err = io.Copy(objFile, objDataBuf)
		if err != nil {
			log.Println("[ERR] FileSyncClient.fetchResource() :  cannot save 2 file : ", sUri, err.Error())
			return
		}

		sLocalPath = sLocalFile    // local file path
		nTaskStatus = ST_Completed // set complete flag
	}
}

// [method] login 2 server
func (pSelf *FileSyncClient) login2Server() bool {
	// generate Login Url string
	var sUrl string = fmt.Sprintf("http://%s/login?account=%s&password=%s", pSelf.ServerHost, pSelf.Account, pSelf.Password)
	log.Printf("[INF] FileSyncClient.login2Server() : http://%s/login?account=%s", pSelf.ServerHost, pSelf.Account)

	// declare http request variable
	httpClient := http.Client{
		CheckRedirect: nil,
		Jar:           globalCurrentCookieJar,
	}
	httpReq, err := http.NewRequest("GET", sUrl, nil)
	httpRes, err := httpClient.Do(httpReq)

	if err != nil {
		log.Println("[ERR] FileSyncClient.login2Server() :  error in response : ", sUrl, err.Error())
		return false
	}

	// parse && read response string
	defer httpRes.Body.Close()
	body, err := ioutil.ReadAll(httpRes.Body)
	if err != nil {
		log.Println("[ERR] FileSyncClient.login2Server() :  cannot read response : ", sUrl, err.Error())
		return false
	}

	// set the current cookies
	globalCurrentCookies = globalCurrentCookieJar.Cookies(httpReq.URL)

	// check xml result in response
	var xmlRes struct {
		XMLName xml.Name `xml:"login"`
		Result  struct {
			XMLName xml.Name `xml:"result"`
			Status  string   `xml:"status,attr"`
			Desc    string   `xml:"desc,attr"`
		}
	} // Build Response Xml Structure

	// Unmarshal Obj From Xml String
	if err := xml.Unmarshal(body, &xmlRes); err != nil {
		log.Println("[ERR] FileSyncClient.login2Server() : ", err.Error())
		log.Println("[ERR] FileSyncClient.login2Server() : ", string(body))
	} else {
		if strings.ToLower(xmlRes.Result.Status) == "success" {
			return true
		}

		log.Println("[WARN] FileSyncClient.login2Server() : ", string(body))
	}

	return false
}

// [method] list resources
func (pSelf *FileSyncClient) fetchResList(objResourceList *ResourceList) bool {
	// generate list Url string
	var sUrl string = fmt.Sprintf("http://%s/list", pSelf.ServerHost)
	log.Println("[INF] FileSyncClient.fetchResList() : /list")

	// declare http request variable
	httpClient := http.Client{
		CheckRedirect: nil,
		Jar:           globalCurrentCookieJar,
	}
	httpReq, err := http.NewRequest("GET", sUrl, nil)
	httpRes, err := httpClient.Do(httpReq)

	if err != nil {
		log.Println("[ERR] FileSyncClient.fetchResList() : error in response : ", sUrl, err.Error())
		return false
	}

	// parse && read response string
	defer httpRes.Body.Close()
	body, err := ioutil.ReadAll(httpRes.Body)
	if err != nil {
		log.Println("[ERR] FileSyncClient.fetchResList() : cannot read response : ", sUrl, err.Error())
		return false
	}

	// unmarshal obj. from xml string
	if err := xml.Unmarshal(body, &objResourceList); err != nil {
		log.Println("[ERR] FileSyncClient.fetchResList() : ", err.Error())
		log.Println("[ERR] FileSyncClient.fetchResList() : ", string(body))

		return false
	} else {
		if len(objResourceList.Download) <= 0 {
			log.Println("[WARN] FileSyncClient.fetchResList() : resource list is empty : ", string(body))
			return false
		}

		return true
	}
}

// [method] dump progress status
func (pSelf *FileSyncClient) dumpProgress(nAddRef int) bool {
	var objXmlProgress struct {
		XMLName    xml.Name `xml:"progress"`
		Percentage struct {
			XMLName   xml.Name `xml:"percentage"`
			TotalTask int      `xml:"taskcount,attr"`
			Progress  float32  `xml:"taskprogress,attr"`
			Update    string   `xml:"update,attr"`
		}
	}

	pSelf.CompleteCount = pSelf.CompleteCount + nAddRef
	objXmlProgress.Percentage.TotalTask = pSelf.CompleteCount
	objXmlProgress.Percentage.Progress = float32(pSelf.CompleteCount) / float32(pSelf.TaskCount)
	objXmlProgress.Percentage.Update = time.Now().Format("2006-01-02 15:04:05")

	if sResponse, err := xml.Marshal(&objXmlProgress); err != nil {
		log.Println("[ERR] FileSyncClient.dumpProgress() : cannot marshal xml object 2 string : ", err.Error())
	} else {
		objFile, err := os.Create(pSelf.ProgressFile)
		defer objFile.Close()
		if err != nil {
			log.Println("[ERR] FileSyncClient.dumpProgress() : cannot create progress file : ", pSelf.ProgressFile, err.Error())
			return false
		}

		log.Println("[INF] FileSyncClient.dumpProgress() : progress : ", string(sResponse))
		objFile.WriteString(xml.Header)
		objFile.Write(sResponse)

		return true
	}

	return false
}
