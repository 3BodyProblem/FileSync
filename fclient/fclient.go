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
	"net"
	"net/http"
	"net/http/cookiejar"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
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
	TYPE    string   `xml:"type,attr"`
	URI     string   `xml:"uri,attr"`
	MD5     string   `xml:"md5,attr"`
	UPDATE  string   `xml:"update,attr"`
}

type ResourceList struct {
	XMLName  xml.Name      `xml:"resource"`
	Download []ResDownload `xml:"download"`
}

///////////////////////////////////// HTTP Client Engine Stucture/Class

type DataSeq struct {
	LastSeqNo      int                 // Last Sequence No
	NoCount        int                 // Number Of Resource
	UnusedFlag     bool                // UnUsed Flag
	UncompressFlag bool                // Undo Flag
	TaskChannel    chan int            // Channel Of Actived Task
	ResFileChannel chan DownloadStatus // Channel Of Download Task
}

type FileSyncClient struct {
	ServerHost    string             // Server IP + Port
	Account       string             // Server Login Username
	Password      string             // Server Login Password
	TTL           int                // Time To Live
	ProgressFile  string             // Progress File Path
	TaskCount     int                // Task Count
	CompleteCount int                // Task Complete Count
	objSeqLock    *sync.Mutex        // Data Seq Map Locker
	objMapDataSeq map[string]DataSeq // Map Of Last Sequence No
	StopFlagFile  string             // Stop Flag File Path
}

///////////////////////////////////// [OutterMethod]
//  Active HTTP Client
func (pSelf *FileSyncClient) DoTasks(sTargetFolder string) bool {
	var nEnd int = 0
	var nBegin int = 0
	var sCurDataType string = ""
	var objResourceList ResourceList // uri list object
	log.Println("[INF] FileSyncClient.DoTasks() : .................. Executing Tasks .................. ")
	pSelf.objSeqLock = new(sync.Mutex)
	pSelf.objMapDataSeq = make(map[string]DataSeq)
	pSelf.dumpProgress(0)
	if false == pSelf.login2Server() { ////////////////////// Login 2 Server
		return false
	}

	if false == pSelf.fetchResList(&objResourceList) { ////// List Resource Table
		return false
	}

	///////////////////// Dispatch Downloading Tasks
	pSelf.TaskCount = len(objResourceList.Download)
	for i, objRes := range objResourceList.Download {
		if i == 0 {
			sCurDataType = objRes.TYPE
		}
		if sCurDataType != objRes.TYPE {
			nEnd = i
			log.Printf("[INF] FileSyncClient.DoTasks() : DataType: %s(%s) %d~%d, len=%d", sCurDataType, objRes.TYPE, nBegin, nEnd, len(objResourceList.Download[nBegin:nEnd]))
			go pSelf.DownloadResources(sCurDataType, sTargetFolder, objResourceList.Download[nBegin:nEnd])
			nBegin = i
			sCurDataType = objRes.TYPE
		}
	}

	if len(objResourceList.Download[nBegin:]) > 0 {
		log.Printf("[INF] FileSyncClient.DoTasks() : DataType: %s %d~%d, len=%d", sCurDataType, nBegin, len(objResourceList.Download), len(objResourceList.Download[nBegin:]))
		go pSelf.DownloadResources(sCurDataType, sTargetFolder, objResourceList.Download[nBegin:])
	}
	///////////////////////////// Check Tasks Status //////////////////////////////
	for i := 0; i < pSelf.TTL && pSelf.CompleteCount < pSelf.TaskCount; i++ {
		time.Sleep(1 * time.Second)

		if pSelf.StopFlagFile != "" {
			objStopFlag, err := os.Open(pSelf.StopFlagFile)
			if nil == err {
				objStopFlag.Close()
				log.Println("[WARN] FileSyncServer.DoTasks() : program terminated by stop flag file : ", pSelf.StopFlagFile)
				err := os.Remove(pSelf.StopFlagFile)
				if err != nil {
					log.Println("[WARN] FileSyncServer.DoTasks() : cannot remove stop flag file :", pSelf.StopFlagFile)
				}
				os.Exit(100)
			}

		}
	}

	pSelf.dumpProgress(0)
	time.Sleep(time.Second * 3)
	log.Println("[INF] FileSyncClient.DoTasks() : ................ Mission Completed ................... ")
	return true
}

func (pSelf *FileSyncClient) ExtractResData(sTargetFolder string, objResInfo DownloadStatus) {
	for bLoop := true; true == bLoop; {
		pSelf.objSeqLock.Lock()
		if objDataSeq, ok := pSelf.objMapDataSeq[objResInfo.DataType]; ok {
			pSelf.objSeqLock.Unlock()

			if (objDataSeq.LastSeqNo + 1) < objResInfo.SeqNo {
				time.Sleep(time.Second)
			} else {
				bLoop = false
				///////////// Uncompress Resource File ///////////////////////////
				objUnzip := Uncompress{TargetFolder: sTargetFolder}
				if false == objUnzip.Unzip(objResInfo.LocalPath, objResInfo.URI) {
					os.Remove(objResInfo.LocalPath)
					log.Println("[ERROR] FileSyncClient.ExtractResData() :  error in uncompression : ", objResInfo.URI)
					os.Exit(-100)
					return
				}

				pSelf.dumpProgress(1)
				log.Printf("[INF] FileSyncClient.ExtractResData() : [DONE] [%s, %d-->%d] -----------> %s (%d/%d)", objResInfo.DataType, objResInfo.SeqNo, objDataSeq.NoCount, objResInfo.URI, pSelf.CompleteCount, pSelf.TaskCount)

				pSelf.objSeqLock.Lock()
				objDataSeq.LastSeqNo = objResInfo.SeqNo
				objDataSeq.UncompressFlag = false
				pSelf.objMapDataSeq[objResInfo.DataType] = objDataSeq
				pSelf.objSeqLock.Unlock()
			}

			continue
		}

		pSelf.objSeqLock.Unlock()
	}
}

func (pSelf *FileSyncClient) DownloadResources(sDataType string, sTargetFolder string, lstDownloadTask []ResDownload) {
	var refTaskChannel chan int
	var refResFileChannel chan DownloadStatus
	var nMaxDownloadThread int = 6

	if len(lstDownloadTask) > 100 {
		nMaxDownloadThread = 10
	}

	for i, objRes := range lstDownloadTask {
		/////////////////////////////// Arouse Downloading Tasks ////////////////////////////
		pSelf.objSeqLock.Lock() // Lock
		if _, ok := pSelf.objMapDataSeq[objRes.TYPE]; ok {
		} else {
			pSelf.objMapDataSeq[objRes.TYPE] = DataSeq{LastSeqNo: (i - 1), TaskChannel: make(chan int, nMaxDownloadThread), ResFileChannel: make(chan DownloadStatus, 6), NoCount: len(lstDownloadTask), UnusedFlag: true, UncompressFlag: true}
		}
		refTaskChannel = pSelf.objMapDataSeq[objRes.TYPE].TaskChannel
		refResFileChannel = pSelf.objMapDataSeq[objRes.TYPE].ResFileChannel
		pSelf.objSeqLock.Unlock() // Unlock

		refTaskChannel <- i // WAIT 2 Engage 1 Channel Resource
		go pSelf.fetchResource(objRes.TYPE, objRes.URI, objRes.MD5, objRes.UPDATE, sTargetFolder, i, refTaskChannel)
		///////////////////////////// Check Extraction Tasks //////////////////////////////
		for j := 0; j < pSelf.TTL && pSelf.CompleteCount < pSelf.TaskCount; {
			select {
			case objStatus := <-refResFileChannel:
				if objStatus.Status == ST_Error {
					log.Println("[ERROR] FileSyncClient.DownloadResources() : error in downloading resource : ", objRes.TYPE, i)
					log.Println("[ERROR] FileSyncClient.DownloadResources() : ----------------- Mission Terminated!!! ------------------")
					os.Exit(-100)
				}

				if objStatus.Status == ST_Completed {
					go pSelf.ExtractResData(sTargetFolder, objStatus)
				}

				if objStatus.Status == ST_Ignore {
					pSelf.dumpProgress(1)
					pSelf.objSeqLock.Lock()
					objDataSeq, _ := pSelf.objMapDataSeq[objStatus.DataType]
					objDataSeq.LastSeqNo = objStatus.SeqNo
					objDataSeq.UncompressFlag = false
					pSelf.objMapDataSeq[objStatus.DataType] = objDataSeq
					pSelf.objSeqLock.Unlock()
				}
			default:
				if (len(refTaskChannel) + len(refResFileChannel)) == 0 {
					j = pSelf.TTL + 10
				}
				time.Sleep(1 * time.Second)
			}

			if (i + 1) < len(lstDownloadTask) {
				break
			}
		}
	}

	log.Printf("[INF] FileSyncClient.DownloadResources() : [Release Downloader] %s : TaskCount = %d", sDataType, len(lstDownloadTask))
}

///////////////////////////////////// [InnerMethod]
// [method] download resource
type DownloadStatus struct {
	DataType  string         // Data Type Name
	URI       string         // download url
	Status    TaskStatusType // task status
	LocalPath string         // File Path In Disk
	SeqNo     int            // Sequence No
}

func (pSelf *FileSyncClient) fetchResource(sDataType, sUri, sMD5, sDateTime, sTargetFolder string, nSeqNo int, objTaskChannel chan int) bool {
	var sLocalPath string = ""
	var nTaskStatus TaskStatusType = ST_Error // Mission Terminated!
	var objFCompare FComparison = FComparison{URI: sUri, MD5: sMD5, DateTime: sDateTime}

	defer func() bool {
		var refTaskChannel chan int
		var refResFileChannel chan DownloadStatus

		for bLoop := true; true == bLoop; {
			pSelf.objSeqLock.Lock()
			if objDataSeq, ok := pSelf.objMapDataSeq[sDataType]; ok {
				refTaskChannel = pSelf.objMapDataSeq[sDataType].TaskChannel
				refResFileChannel = pSelf.objMapDataSeq[sDataType].ResFileChannel
				pSelf.objSeqLock.Unlock()

				if (objDataSeq.LastSeqNo + 1) < nSeqNo {
					time.Sleep(time.Second)
				} else {
					bLoop = false
					if nTaskStatus == ST_Completed {
						log.Printf("[INF] FileSyncClient.fetchResource() : [√] %s:%d->%d => %s (Running:%d)", sDataType, objDataSeq.LastSeqNo, nSeqNo, sUri, len(objTaskChannel))
					} else if nTaskStatus == ST_Ignore {
						log.Printf("[INF] FileSyncClient.fetchResource() : [Ignore] %s:%d->%d => %s (Running:%d)", sDataType, objDataSeq.LastSeqNo, nSeqNo, sUri, len(objTaskChannel))
					} else if nTaskStatus == ST_Error {
						log.Printf("[WARN] FileSyncClient.fetchResource() : [×] %s:%d->%d Deleting File: => %s (Running:%d)", sDataType, objDataSeq.LastSeqNo, nSeqNo, sUri, len(objTaskChannel))
						os.Remove(sLocalPath)
					}

					pSelf.objSeqLock.Lock()
					objDataSeq.UncompressFlag = true
					pSelf.objMapDataSeq[sDataType] = objDataSeq
					pSelf.objSeqLock.Unlock()
				}

				continue
			}

			pSelf.objSeqLock.Unlock()
		}

		<-refTaskChannel
		refResFileChannel <- DownloadStatus{DataType: sDataType, URI: sUri, Status: nTaskStatus, LocalPath: sLocalPath, SeqNo: nSeqNo} // Mission Finished!
		return true
	}()

	if true == objFCompare.Compare() {
		nTaskStatus = ST_Ignore // Mission Ignored!
	} else {
		// parse && read response string
		var sUrl string = fmt.Sprintf("http://%s/get?uri=%s", pSelf.ServerHost, sUri)
		httpClient := http.Client{
			CheckRedirect: nil,
			Jar:           globalCurrentCookieJar,
			Timeout:       10 * 60 * time.Second,
			Transport: &http.Transport{
				Dial: func(netw, addr string) (net.Conn, error) {
					conn, err := net.DialTimeout(netw, addr, time.Second*60*10)
					if err != nil {
						return nil, err
					}
					conn.SetDeadline(time.Now().Add(time.Second * 60 * 10))
					return conn, nil
				},
				// TLSHandshakeTimeout:   time.Second * 10,
				ResponseHeaderTimeout: time.Second * 60 * 1,
				ExpectContinueTimeout: time.Second * 60 * 1,
			},
		}

		var httpRes *http.Response = nil
		httpReq, err := http.NewRequest("GET", sUrl, nil)
		for i := 0; i < 10; i++ {
			httpRes, err = httpClient.Do(httpReq)
			if err != nil {
				log.Println("[ERR] FileSyncClient.fetchResource() :  error in response : ", err.Error())
				log.Printf("[INF] FileSyncClient.fetchResource() : Download Again!!! URL=%s, MD5=%s, DownloadTimes=(%d)", sUrl, sMD5, i)
				continue
			}

			break
		}

		// get absolute file path
		defer httpRes.Body.Close()
		sLocalFolder, err := filepath.Abs((filepath.Dir("./")))
		if err != nil {
			log.Println("[WARN] FileSyncClient.fetchResource() : failed 2 fetch absolute path of program", sUrl, sMD5, sDateTime)
			return false
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
			return false
		}

		objDataBuf := &bytes.Buffer{}
		_, err2 := objDataBuf.ReadFrom(httpRes.Body)
		if err2 != nil {
			log.Println("[ERR] FileSyncClient.fetchResource() :  cannot read response : ", sUrl, sMD5, sDateTime, err.Error())
			return false
		}

		// save 2 local file
		objFile, _ := os.Create(sLocalFile)
		defer objFile.Close()
		_, err = io.Copy(objFile, objDataBuf)
		if err != nil {
			log.Println("[ERR] FileSyncClient.fetchResource() :  cannot save 2 file : ", sUri, sMD5, sDateTime, err.Error())
			return false
		}

		sLocalPath = sLocalFile    // local file path
		nTaskStatus = ST_Completed // set complete flag
	}

	return true
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
		Timeout:       8 * time.Second,
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

		log.Println("[ERR] FileSyncClient.login2Server() : ", string(body))
		log.Println("[ERR] FileSyncClient.login2Server() : logon failure : invalid accountid or password, u r not allowed 2 logon the server.")
	}

	return false
}

// [method] list resources
func (pSelf *FileSyncClient) fetchResList(objResourceList *ResourceList) bool {
	// generate list Url string
	var sUrl string = fmt.Sprintf("http://%s/list", pSelf.ServerHost)
	log.Println("[INF] FileSyncClient.fetchResList() : [GET] /list")

	// declare http request variable
	httpClient := http.Client{
		CheckRedirect: nil,
		Jar:           globalCurrentCookieJar,
		Timeout:       time.Second * 10,
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

		objFile.WriteString(xml.Header)
		objFile.Write(sResponse)

		return true
	}

	return false
}
