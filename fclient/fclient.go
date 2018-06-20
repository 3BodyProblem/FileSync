/**
 * @brief		下载引擎
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
	//"runtime/pprof"
	"strings"
	"time"
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

///////////////////////////////////// 下载资源清单列表结构 /////////////////////////
/**
 * @Class 		ResDownload
 * @brief		某一项具体的下载资源项信息
 * @author		barry
 */
type ResDownload struct {
	XMLName xml.Name `xml:"download"`
	TYPE    string   `xml:"type,attr"`   // 数据类型 sse.d1 / szse.m60
	URI     string   `xml:"uri,attr"`    // 资源URI路径
	MD5     string   `xml:"md5,attr"`    // 资源文件的MD5
	UPDATE  string   `xml:"update,attr"` // 资源文件的生成日期
}

/**
 * @Class 		ResourceList
 * @brief		下载资源清单表
 * @author		barry
 */
type ResourceList struct {
	XMLName  xml.Name      `xml:"resource"`
	Download []ResDownload `xml:"download"` // 可下载资源清单表
}

///////////////////////////////////// 资源下载同步类 /////////////////////////////

/**
 * @Class 		I_Downloader
 * @brief		下载管理器接口
 * @author		barry
 */
type I_Downloader interface {
	/**
	 * @brief		当前下载进度存盘更新
	 * @param[in]	nAddRef		进度值，正负偏移量
	 * @return		true		成功
	 */
	dumpProgress(nAddRef int) bool

	/**
	 * @brief		下载资源文件
	 * @param[in]	sDataType 		资源文件类型
	 * @param[in]	sUri			资源文件URI标识
	 * @param[in]	sMD5			资源文件MD5串
	 * @param[in]	sDateTime		资源文件在服务端的生成时间
	 */
	FetchResource(sDataType, sUri, sMD5, sDateTime string) (TaskStatusType, string)

	/**
	 * @brief		获取资源下载任务的完成度百分比
	 */
	GetPercentageOfTasks() float32
}

/**
 * @Class 		FileSyncClient
 * @brief		资源文件下载类
 * @author		barry
 */
type FileSyncClient struct {
	ServerHost       string                  // Server IP + Port
	Account          string                  // Server Login Username
	Password         string                  // Server Login Password
	TTL              int                     // Time To Live
	nRetryTimes      int                     // Retry Times
	objCacheTable    CacheFileTable          // Table Of Download Resources
	ProgressFile     string                  // Progress File Path
	TotalTaskCount   int                     // 同步任务文件总数
	CompleteCount    int                     // 同步任务完成数
	StopFlagFile     string                  // Stop Flag File Path
	DownloadURI      string                  // Resource's URI 4 Download
	objSyncTaskTable map[string]DownloadTask // Map Of Last Sequence No
}

///< ---------------------- [Public 方法] -----------------------------
/**
 * @brief		初始化下载客户端
 */
func (pSelf *FileSyncClient) Initialize() bool {
	pSelf.nRetryTimes = 3
	pSelf.objSyncTaskTable = make(map[string]DownloadTask)
	pSelf.objCacheTable.Initialize()

	return true
}

/**
 * @brief		根据输入参数执行下载任务
 * @param[in]	sTargetFolder		下载资源文件的根目录
 */
func (pSelf *FileSyncClient) DoTasks(sTargetFolder string) bool {
	var nBegin, nEnd int = 0, 0
	var sCurDataType string = ""
	var objResourceList ResourceList // uri list object
	var nMaxDownloadThread int = 2   // 下载任务栈长度(并发下载数)
	var nMaxExtractThread int = 5    // 解压任务栈长度(并发下载数)
	log.Println("[INF] FileSyncClient.DoTasks() : .................. Executing Tasks .................. ")
	/////// 本程序进行性能测试的代码，用于找出哪个函数最慢 /////////////////
	/*f, err := os.Create("performace_test_client.dat")
	if err != nil {
		log.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()*/
	pSelf.dumpProgress(0)
	if false == pSelf.login2Server() { ////////////////////// 登录到服务器
		return false
	}

	if false == pSelf.fetchResList(&objResourceList) { ////// 获取下载资源清单表
		return false
	}

	///////////////////// 启动下载器 & 分配下载任务 ///////////////////////
	pSelf.TotalTaskCount = len(objResourceList.Download)
	for i, objRes := range objResourceList.Download {
		if i == 0 {
			sCurDataType = objRes.TYPE
		}

		if sCurDataType != objRes.TYPE {
			nEnd = i
			lstDownloadTableOfType := objResourceList.Download[nBegin:nEnd]
			log.Printf("[INF] FileSyncClient.DoTasks() : DataType: %s(%s) %d~%d, len=%d", sCurDataType, objRes.TYPE, nBegin, nEnd, len(lstDownloadTableOfType))
			pSelf.objSyncTaskTable[sCurDataType] = DownloadTask{I_CacheMgr: &(pSelf.objCacheTable), I_Downloader: pSelf, TTL: pSelf.TTL, RetryTimes: pSelf.nRetryTimes, LastSeqNo: -1, ParallelDownloadChannel: make(chan int, nMaxDownloadThread), ResFileChannel: make(chan DownloadStatus, nMaxExtractThread), NoCount: len(lstDownloadTableOfType)}
			if objDownloadTask, ok := pSelf.objSyncTaskTable[sCurDataType]; ok {
				go objDownloadTask.DownloadResourcesByCategory(sCurDataType, sTargetFolder, objResourceList.Download[nBegin:nEnd])
			}
			nBegin = i
			sCurDataType = objRes.TYPE
		}
	}

	lstDownloadTableOfType := objResourceList.Download[nBegin:nEnd]
	if len(lstDownloadTableOfType) > 0 {
		log.Printf("[INF] FileSyncClient.DoTasks() : DataType: %s %d~%d, len=%d", sCurDataType, nBegin, len(objResourceList.Download), len(objResourceList.Download[nBegin:]))
		pSelf.objSyncTaskTable[sCurDataType] = DownloadTask{I_CacheMgr: &(pSelf.objCacheTable), I_Downloader: pSelf, TTL: pSelf.TTL, RetryTimes: pSelf.nRetryTimes, LastSeqNo: -1, ParallelDownloadChannel: make(chan int, nMaxDownloadThread), ResFileChannel: make(chan DownloadStatus, nMaxExtractThread), NoCount: len(lstDownloadTableOfType)}
		if objDownloadTask, ok := pSelf.objSyncTaskTable[sCurDataType]; ok {
			go objDownloadTask.DownloadResourcesByCategory(sCurDataType, sTargetFolder, objResourceList.Download[nBegin:])
		}
	}

	////////// 检查各下载任务是否完成 & 是否出现异常需要下载资源文件的回滚 //////////////
	for i := 0; i < pSelf.TTL && pSelf.CompleteCount < pSelf.TotalTaskCount; i++ {
		time.Sleep(1 * time.Second)
		pSelf.objCacheTable.RollbackUnextractedCacheFilesAndExit() // 判断是否可以回滚下载的资源文件

		if pSelf.StopFlagFile != "" { // 判断是否出现退出标识的文件
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
	log.Println("[INF] FileSyncClient.DoTasks() : ................ Mission Completed ................... ")
	time.Sleep(time.Second * 3)
	return true
}

/**
 * @brief		下载资源文件
 * @param[in]	sDataType 		资源文件类型
 * @param[in]	sUri			资源文件URI标识
 * @param[in]	sMD5			资源文件MD5串
 * @param[in]	sDateTime		资源文件在服务端的生成时间
 */
func (pSelf *FileSyncClient) FetchResource(sDataType, sUri, sMD5, sDateTime string) (TaskStatusType, string) {
	var sUrl string = fmt.Sprintf("http://%s/get?uri=%s", pSelf.ServerHost, sUri)        // 资源下载的URL串
	var sLocalPath string = ""                                                           // 下载资源的本地缓存文件路径
	var httpRes *http.Response = nil                                                     // 下载资源的请求返回对象(Response)
	var nTaskStatus TaskStatusType = ST_Error                                            // 下载任务成功状态（返回值）
	var objFCompare FComparison = FComparison{URI: sUri, MD5: sMD5, DateTime: sDateTime} // 待下载资源与本地缓存文件的差异比较对象

	defer func() {
		if pObjPanic := recover(); pObjPanic != nil { // 异常恢复，以至于程序不会异常中断
			sLocalPath = ""
			nTaskStatus = ST_Error
			log.Println("[ERR] FileSyncClient.FetchResource() : [panic] exception --> ", sUri, pObjPanic)
			return
		}
	}()

	if true == objFCompare.Compare() { // 和缓存文件相同，已经下载过了，跳空以下的所有资源下载逻辑
		nTaskStatus = ST_Ignore
	} else { // 和本地缓存文件不同（或缓存文件不存在），需要从服务器下载
		httpClient := http.Client{
			CheckRedirect: nil,
			Jar:           globalCurrentCookieJar,
			Timeout:       6 * 60 * time.Second,
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 6 * 60 * time.Second,
				}).Dial,
				// TLSHandshakeTimeout:time.Second * 10,
				// IdleConnTimeout:    time.Second * 30 * 1,
				ResponseHeaderTimeout: time.Second * 30 * 1,
				ExpectContinueTimeout: time.Second * 30 * 1,
			},
		}
		/////////////// 请求下载的资源数据 /////////////////////
		httpReq, err := http.NewRequest("GET", sUrl, nil)
		httpRes, err = httpClient.Do(httpReq)
		if err != nil {
			log.Println("[ERR] FileSyncClient.FetchResource() : error in response : ", err.Error())
			return ST_Error, ""
		}

		defer httpRes.Body.Close()
		////////////// 为下载的资源文件准备好目录结构进行存放 ////
		sLocalFolder, err := filepath.Abs((filepath.Dir("./")))
		if err != nil {
			log.Println("[WARN] FileSyncClient.FetchResource() : failed 2 fetch absolute path of program", sUrl, sMD5, sDateTime)
			return ST_Error, ""
		}

		sLocalFolder = filepath.Join(sLocalFolder, CacheFolder)
		sLocalFile := filepath.Join(sLocalFolder, sUri)
		sMkFolder := path.Dir(sLocalFile)
		if "windows" == runtime.GOOS {
			sMkFolder = sLocalFile[:strings.LastIndex(sLocalFile, "\\")]
		}
		err = os.MkdirAll(sMkFolder, 0711)
		if err != nil {
			log.Printf("[WARN] FileSyncClient.FetchResource() : failed 2 create folder : %s : %s", sLocalFile, err.Error())
			return ST_Error, ""
		}
		////////////// 从网卡读出资源文件数据，并存盘 ////////////
		objDataBuf := &bytes.Buffer{}
		_, err2 := objDataBuf.ReadFrom(httpRes.Body)
		if err2 != nil {
			log.Println("[ERR] FileSyncClient.FetchResource() :  cannot read response : ", sUrl, sMD5, sDateTime, err.Error())
			return ST_Error, ""
		}

		objFile, _ := os.Create(sLocalFile)
		defer objFile.Close()
		_, err = io.Copy(objFile, objDataBuf)
		if err != nil {
			log.Println("[ERR] FileSyncClient.FetchResource() :  cannot save 2 file : ", sUri, sMD5, sDateTime, err.Error())
			return ST_Error, ""
		}
		//////////// 设置下载的资源文件信息，并待返回
		sLocalPath = sLocalFile    // 本地资源文件存放路径
		nTaskStatus = ST_Completed // 本次下载成功标识
	}

	return nTaskStatus, sLocalPath
}

/**
 * @brief		登录到服务器
 */
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

/**
 * @brief		获取可下载的资源清单表
 * @param[out]	objResourceList		资源清单表
 */
func (pSelf *FileSyncClient) fetchResList(objResourceList *ResourceList) bool {
	if pSelf.DownloadURI != "" {
		// download uri resource only
		var objDownload ResDownload

		if strings.Contains(pSelf.DownloadURI, "MIN1_TODAY") == true && strings.Contains(pSelf.DownloadURI, "SSE") {
			objDownload.TYPE = "sse.real_m1"
			objDownload.URI = "SyncFolder/SSE/MIN1_TODAY/MIN1_TODAY"
			objDownload.MD5 = "none"
			objResourceList.Download = append(objResourceList.Download, objDownload)
			return true
		} else if strings.Contains(pSelf.DownloadURI, "MIN1_TODAY") == true && strings.Contains(pSelf.DownloadURI, "SZSE") {
			objDownload.TYPE = "szse.real_m1"
			objDownload.URI = "SyncFolder/SZSE/MIN1_TODAY/MIN1_TODAY"
			objDownload.MD5 = "none"
			objResourceList.Download = append(objResourceList.Download, objDownload)
			return true
		}

		return false
	}

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

/**
 * @brief		获取资源下载任务的完成度百分比
 */
func (pSelf *FileSyncClient) GetPercentageOfTasks() float32 {
	return float32(pSelf.CompleteCount) / float32(pSelf.TotalTaskCount) * 100
}

/**
 * @brief		当前下载进度存盘更新
 * @param[in]	nAddRef		进度值，正负偏移量
 * @return		true		成功
 */
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
	objXmlProgress.Percentage.Progress = float32(pSelf.CompleteCount) / float32(pSelf.TotalTaskCount)
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
