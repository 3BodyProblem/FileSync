/**
 * @brief		Engine Of Client
 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/cookiejar"
	"os"
	"path/filepath"
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
	ServerHost string              // Server IP + Port
	Account    string              // Server Login Username
	Password   string              // Server Login Password
	TTL        int                 // Time To Live
	objChannel chan DownloadStatus // Channel Of Download Task
}

///////////////////////////////////// [OutterMethod]
//  Active HTTP Client
func (pSelf *FileSyncClient) DoTasks() {
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
		go pSelf.fetchResource(objRes.URI, objRes.MD5, objRes.UPDATE)
	}

	///////////////////////////// Check Tasks Status //////////////////////////////
	log.Println("[INF] FileSyncClient.DoTasks() : Task Number = ", len(objMapTask))
	for i := 0; i < pSelf.TTL; i++ {
		select {
		case objStatus := <-pSelf.objChannel:
			if _, ok := objMapTask[objStatus.URI]; ok {
				if objStatus.Status == ST_Completed || objStatus.Status == ST_Ignore {
					objMapTask[objStatus.URI] = true // mark up: task completed
					if objStatus.Status == ST_Completed {
						log.Println("[INF] FileSyncClient.DoTasks() : [Downloaded] -->", objStatus.URI)
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

	log.Println("[INF] FileSyncClient.DoTasks() : Mission Completed ...... ")
}

///////////////////////////////////// [InnerMethod]
// [method] download resource
type DownloadStatus struct {
	URI    string         // download url
	Status TaskStatusType // task status
}

func (pSelf *FileSyncClient) fetchResource(sUri, sMD5, sDateTime string) {
	var objFCompare FComparison = FComparison{URI: sUri, MD5: sMD5, DateTime: sDateTime}

	if true == objFCompare.Compare() {
		log.Println("[INF] FileSyncClient.fetchResource() : [Ignore] -->", sUri, sMD5, sDateTime)
		pSelf.objChannel <- DownloadStatus{URI: sUri, Status: ST_Ignore} // Mission Ignored!
	} else {
		log.Println("[INF] FileSyncClient.fetchResource() : [Downloading] -->", sUri, sMD5, sDateTime)

		// generate list Url string
		var sUrl string = fmt.Sprintf("http://%s/get?uri=%s", pSelf.ServerHost, sUri)
		log.Println("[INF] FileSyncClient.fetchResource() : ", sUrl)

		// declare http request variable
		httpClient := http.Client{
			CheckRedirect: nil,
			Jar:           globalCurrentCookieJar,
		}
		httpReq, err := http.NewRequest("GET", sUrl, nil)
		httpRes, err := httpClient.Do(httpReq)
		if err != nil {
			log.Println("[ERR] FileSyncClient.fetchResource() :  error in response : ", sUrl, err.Error())
			return
		}

		// parse && read response string
		defer httpRes.Body.Close()
		body, err := ioutil.ReadAll(httpRes.Body)
		if err != nil {
			log.Println("[ERR] FileSyncClient.fetchResource() :  cannot read response : ", sUrl, err.Error())
			return
		}

		// restore response file 2 resource folder
		log.Println("[INF] FileSyncClient.fetchResource() : ", body)

		// get absolute file path
		sLocalFolder, err := filepath.Abs((filepath.Dir("./")))
		if err != nil {
			log.Println("[WARN] FileSyncClient.fetchResource() : failed 2 fetch absolute path of program")
			pSelf.objChannel <- DownloadStatus{URI: sUri, Status: ST_Error} // Mission Terminated!
			return
		}

		sLocalFolder = filepath.Join(sLocalFolder, CacheFolder)
		sLocalFile := filepath.Join(strings.LastIndex(sLocalFolder, "/"), sUri)

		err = os.MkdirAll(sLocalFile, 0777)
		if err != nil {
			log.Printf("[WARN] FileSyncClient.fetchResource() : failed 2 create folder : %s : %s", sLocalFile, err.Error())
			return
		}

		file, _ := os.Create(sLocalFile)
		io.Copy(file, httpRes.Body)

		log.Println("[INF] FileSyncClient.fetchResource() : [Complete] -->", sLocalFile)
		pSelf.objChannel <- DownloadStatus{URI: sUri, Status: ST_Completed} // Mission Complete!
	}
}

// [method] login 2 server
func (pSelf *FileSyncClient) login2Server() bool {
	// generate Login Url string
	var sUrl string = fmt.Sprintf("http://%s/login?account=%s&password=%s", pSelf.ServerHost, pSelf.Account, pSelf.Password)
	log.Println("[INF] FileSyncClient.login2Server() : /login?account=", pSelf.Account)

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
		log.Println("[ERR] FileSyncClient.fetchResList() :  error in response : ", sUrl, err.Error())
		return false
	}

	// parse && read response string
	defer httpRes.Body.Close()
	body, err := ioutil.ReadAll(httpRes.Body)
	if err != nil {
		log.Println("[ERR] FileSyncClient.fetchResList() :  cannot read response : ", sUrl, err.Error())
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
