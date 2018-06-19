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

///////////////////////////////////// 下载资源的缓存文件管理类 //////////////////////

/**
 * @Class 		CacheFile
 * @brief		某个缓存资源文件描述类
 * @author		barry
 */
type CacheFile struct {
	URI          string // 资源URI路径
	LocalPath    string // 下载的本地资源文件所在路径
	SeqNo        int    // 下载任务的序号(顺序、时序)
	IsExtracted  bool   // 是否已经解压(已经解压的，不会再被删除/回滚)
	FailureCount int    // 下载失败次数统计
}

/**
 * @Class 		CacheFileTable
 * @brief		缓存资源文件清单表管理类 & 支持回滚删除“未解压”的资源,以避免影响下次下载
 * @author		barry
 */
type CacheFileTable struct {
	objLock            *sync.Mutex          // 资源清单锁
	objCacheFilesTable map[string]CacheFile // 资源清单表
	IsNeedRollback     bool                 // 是否需要回滚删除资源文件标识
}

/**
 * @bruef		初始化资源文件清单管理对象
 */
func (pSelf *CacheFileTable) Initialize() bool {
	pSelf.IsNeedRollback = false
	pSelf.objLock = new(sync.Mutex)
	pSelf.objCacheFilesTable = make(map[string]CacheFile)

	return true
}

/**
 * @brief		新增下载资源描述项
 * @param[in]	sUri			资源URI标识串
 * @param[in]	sFilePath		下载的资源本地路径
 * @param[in]	nSeqNo			资源下载任务序号
 */
func (pSelf *CacheFileTable) NewResource(sUri, sFilePath string, nSeqNo int) {
	var nDownloadFailureTimes int = 0

	pSelf.objLock.Lock()
	if objFileInfo, ok := pSelf.objCacheFilesTable[sUri]; ok {
		nDownloadFailureTimes = objFileInfo.FailureCount + 1
		objFileInfo.FailureCount = nDownloadFailureTimes
		pSelf.objCacheFilesTable[sUri] = objFileInfo
	} else {
		pSelf.objCacheFilesTable[sUri] = CacheFile{URI: sUri, LocalPath: sFilePath, SeqNo: nSeqNo, IsExtracted: false, FailureCount: 0}
	}
	pSelf.objLock.Unlock()

	if nDownloadFailureTimes > 9 {
		pSelf.IsNeedRollback = true
		log.Printf("[WARNING] CacheFileTable.NewResource() : download times out of max value: %s", sUri)
	}
}

/**
 * @brief		标记为资源文件“已经解压”
 * @param[in]	sUri		资源URI标识串
 */
func (pSelf *CacheFileTable) MarkExtractedRes(sUri string) {
	pSelf.objLock.Lock()
	if objFileInfo, ok := pSelf.objCacheFilesTable[sUri]; ok {
		objFileInfo.IsExtracted = true
		pSelf.objCacheFilesTable[sUri] = objFileInfo
	}
	pSelf.objLock.Unlock()
}

/**
 * @brief		资源删除、回滚 + 退出当前程序
 * @note 		当IsNeedRollback标识被设置为true时，进行本地缓存文件回滚
 */
func (pSelf *CacheFileTable) RollbackUnextractedCacheFilesAndExit() {
	if false == pSelf.IsNeedRollback {
		return
	}

	for k, v := range pSelf.objCacheFilesTable {
		if false == v.IsExtracted {
			log.Printf("[INF]CacheFileTable.RollbackUnextractedCacheFilesAndExit() : Deleting cache file -> %s (failure times:%d)", v.LocalPath, v.FailureCount)
			os.Remove(v.LocalPath)
			log.Printf("[INF]CacheFileTable.RollbackUnextractedCacheFilesAndExit() : File of Uri: %s, deleted!", k)
		}
	}

	log.Println("[INF] CacheFileTable.RollbackUnextractedCacheFilesAndExit() : ----------------- Mission Terminated!!! ------------------")
	os.Exit(-100)
}

///////////////////////////////////// 资源下载同步类 /////////////////////////////

/**
 * @Class 		DownloadStatus
 * @brief		下载资源的位置描述类
 * @author		barry
 */
type DownloadStatus struct {
	DataType  string         // 下载资源类型
	URI       string         // 下载资源的URI标识
	Status    TaskStatusType // 下载成功状态
	LocalPath string         // 下载资源的本地路径
	SeqNo     int            // 下载任务编号
	MD5       string         // 资源的MD5串
	UPDATE    string         // 资源在服务端的生成时间
}

/**
 * @Class 		DataSeq
 * @brief		资源下载任务序号及相关描述类
 * @author		barry
 */
type DownloadTask struct {
	LastSeqNo               int                 // Last Sequence No
	NoCount                 int                 // Number Of Resource
	UncompressFlag          bool                // Undo Flag
	ParallelDownloadChannel chan int            // 下载任务栈(用来控制最大并发数)
	ResFileChannel          chan DownloadStatus // 解压任务线
}

/**
 * @Class 		FileSyncClient
 * @brief		资源文件下载类
 * @author		barry
 */
type FileSyncClient struct {
	ServerHost    string             // Server IP + Port
	Account       string             // Server Login Username
	Password      string             // Server Login Password
	TTL           int                // Time To Live
	objCacheTable CacheFileTable     // Table Of Download Resources
	ProgressFile  string             // Progress File Path
	nRetryTimes   int                // Retry Times of download
	TaskCount     int                // Task Count
	StopFlagFile  string             // Stop Flag File Path
	DownloadURI   string             // Resource's URI 4 Download
	CompleteCount int                // Task Complete Count
	objSeqLock    *sync.Mutex        // Data Seq Map Locker
	objMapDataSeq map[string]DataSeq // Map Of Last Sequence No
}

///< ---------------------- [Public 方法] -----------------------------
/**
 * @brief		初始化下载客户端
 */
func (pSelf *FileSyncClient) Initialize() bool {
	pSelf.nRetryTimes = 3
	pSelf.objSeqLock = new(sync.Mutex)
	pSelf.objMapDataSeq = make(map[string]DataSeq)
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
	pSelf.TaskCount = len(objResourceList.Download)
	for i, objRes := range objResourceList.Download {
		if i == 0 {
			sCurDataType = objRes.TYPE
		}

		if sCurDataType != objRes.TYPE {
			nEnd = i
			log.Printf("[INF] FileSyncClient.DoTasks() : DataType: %s(%s) %d~%d, len=%d", sCurDataType, objRes.TYPE, nBegin, nEnd, len(objResourceList.Download[nBegin:nEnd]))
			go pSelf.DownloadResourcesByCategory(sCurDataType, sTargetFolder, objResourceList.Download[nBegin:nEnd])
			nBegin = i
			sCurDataType = objRes.TYPE
		}
	}

	if len(objResourceList.Download[nBegin:]) > 0 {
		log.Printf("[INF] FileSyncClient.DoTasks() : DataType: %s %d~%d, len=%d", sCurDataType, nBegin, len(objResourceList.Download), len(objResourceList.Download[nBegin:]))
		go pSelf.DownloadResourcesByCategory(sCurDataType, sTargetFolder, objResourceList.Download[nBegin:])
	}

	////////// 检查各下载任务是否完成 & 是否出现异常需要下载资源文件的回滚 //////////////
	for i := 0; i < pSelf.TTL && pSelf.CompleteCount < pSelf.TaskCount; i++ {
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

///< ---------------------- [Private 方法] -----------------------------

/**
 * @brief		某一类资源文件（列表）的下载器
 * @param[in]	sDataType 		资源类型
 * @param[in]	sTargetFolder	资源解压根目录
 * @param[in]	lstDownloadTask	这一类资源文件的下载清单表
 * @note 		给每个下载任务标一个时序号，然后解压的时候，就按这个顺序来一个一个的解压(保证该类别内资源文件的解压顺序)
 */
func (pSelf *FileSyncClient) DownloadResourcesByCategory(sDataType string, sTargetFolder string, lstDownloadTask []ResDownload) {
	var nExtractedFileNum int = 0             // 在本资源文件类别中，已经解压文件的数量
	var nMaxDownloadThread int = 5            // 下载任务栈长度(并发下载数)
	var refParallelDownloadChannel chan int   // 下载任务栈(每个资源类型对应一个下载任务栈)
	var nMaxExtractThread int = 5             // 解压任务栈长度(并发下载数)
	var refResFileChannel chan DownloadStatus // 解压任务栈(每个资源类型对应一个解压任务栈)
	/////////////////////////// 在该资源类别下，建立分派下载任务 //////////////////////////
	for i, objRes := range lstDownloadTask {
		if strings.Contains(objRes.URI, "shsz_idx_by_date") {
			log.Println("extract............1, enter loop, ", objRes.URI, nExtractedFileNum, len(lstDownloadTask))
		}
		//////////////// 为每个资源类型建立一个下载任务栈 ///////////
		pSelf.objSeqLock.Lock() // Lock
		if strings.Contains(objRes.URI, "shsz_idx_by_date") {
			log.Println("extract............1.1, enter loop, ", objRes.URI, nExtractedFileNum, len(lstDownloadTask))
		}
		if _, ok := pSelf.objMapDataSeq[objRes.TYPE]; !ok {
			pSelf.objMapDataSeq[objRes.TYPE] = DataSeq{LastSeqNo: (i - 1), ParallelDownloadChannel: make(chan int, nMaxDownloadThread), ResFileChannel: make(chan DownloadStatus, nMaxExtractThread), NoCount: len(lstDownloadTask), UncompressFlag: true}
		}
		refParallelDownloadChannel = pSelf.objMapDataSeq[objRes.TYPE].ParallelDownloadChannel
		refResFileChannel = pSelf.objMapDataSeq[objRes.TYPE].ResFileChannel
		if strings.Contains(objRes.URI, "shsz_idx_by_date") {
			log.Println("extract............1.1.1, enter loop, ", objRes.URI, nExtractedFileNum, len(lstDownloadTask))
		}
		pSelf.objSeqLock.Unlock() // Unlock
		if strings.Contains(objRes.URI, "shsz_idx_by_date") {
			log.Println("extract............2, enter loop, ", objRes.URI, nExtractedFileNum, len(lstDownloadTask))
		}
		/////////////// 申请下载任务栈的一个占用名额 ///////////////
		refParallelDownloadChannel <- i
		/////////////// 以同步有序的方式启动下线线程 ///////////////
		go pSelf.StartDataSafetyDownloader(objRes.TYPE, objRes.URI, objRes.MD5, objRes.UPDATE, i, refParallelDownloadChannel, refResFileChannel, pSelf.nRetryTimes)
		////////////////////////// 等待有序的执行该类别中资源的解压任务 /////////////////////
		if strings.Contains(objRes.URI, "shsz_idx_by_date") {
			log.Println("extract............3, enter loop, ", objRes.URI, nExtractedFileNum, len(lstDownloadTask))
		}
		for j := 0; j < pSelf.TTL && nExtractedFileNum < len(lstDownloadTask); {
			select {
			case objStatus := <-refResFileChannel:
				if strings.Contains(objStatus.URI, "shsz_idx_by_date") {
					log.Println("extract, grap a task............4, ", objStatus.Status, objStatus.URI, objStatus.SeqNo)
				}

				if objStatus.Status == ST_Completed { // 增量文件，需要解压
					pSelf.ExtractResData(sTargetFolder, objStatus)
					nExtractedFileNum += 1
				}

				if objStatus.Status == ST_Ignore { // 存量文件，只需忽略
					pSelf.dumpProgress(1)
					pSelf.objSeqLock.Lock()
					objDataSeq, _ := pSelf.objMapDataSeq[objStatus.DataType]
					objDataSeq.LastSeqNo = objStatus.SeqNo
					objDataSeq.UncompressFlag = false
					pSelf.objMapDataSeq[objStatus.DataType] = objDataSeq
					pSelf.objSeqLock.Unlock()
				}

				if objStatus.Status == ST_Error {
					log.Println("[WARN] FileSyncServer.DownloadResourcesByCategory() : error in downloading :", objRes.URI)
				}
			default:
				if (len(refParallelDownloadChannel) + len(refResFileChannel)) == 0 {
					j = pSelf.TTL + 10
				}
				time.Sleep(1 * time.Second)
			}

			if (i + 1) < len(lstDownloadTask) {
				break
			}
		}
		if strings.Contains(objRes.URI, "shsz_idx_by_date") {
			log.Println("extract............5, leave loop, ", objRes.URI)
		}
	}

	log.Printf("[INF] FileSyncClient.DownloadResourcesByCategory() : [Release Downloader] %s : TaskCount = %d", sDataType, len(lstDownloadTask))
}

/**
 * @brief		对本地下载的资源文件进行解压
 * @param[in]	sTargetFolder		下载资源文件解压的根目录
 * @param[in]	objResInfo			下载资源文件位置描述信息
 */
func (pSelf *FileSyncClient) ExtractResData(sTargetFolder string, objResInfo DownloadStatus) {
	if strings.Contains(objResInfo.URI, "shsz_idx_by_date") {
		log.Println("ExtractResData1............, ", objResInfo.Status, objResInfo.URI, objResInfo.SeqNo)
	}

	for bLoop := true; true == bLoop; {
		pSelf.objSeqLock.Lock()
		if objDataSeq, ok := pSelf.objMapDataSeq[objResInfo.DataType]; ok {
			pSelf.objSeqLock.Unlock()

			if (objDataSeq.LastSeqNo + 1) < objResInfo.SeqNo {
				log.Println("ExtractResData () : ..............................., ", objResInfo.URI, objDataSeq.LastSeqNo, objResInfo.SeqNo)
				time.Sleep(time.Second)
			} else {
				bLoop = false
				pSelf.objCacheTable.MarkExtractedRes(objResInfo.URI)
				///////////// Uncompress Resource File ///////////////////////////
				objUnzip := Uncompress{TargetFolder: sTargetFolder}
				if false == objUnzip.Unzip(objResInfo.LocalPath, objResInfo.URI) {
					os.Remove(objResInfo.LocalPath)
					log.Println("[ERROR] FileSyncClient.ExtractResData() :  error in uncompression : ", objResInfo.LocalPath)
					os.Exit(-100)
					return
				}

				pSelf.dumpProgress(1)
				log.Printf("[INF] FileSyncClient.ExtractResData() : [DONE] [%s, %d-->%d] -----------> %s (%d/%d)", objResInfo.DataType, objResInfo.SeqNo, objDataSeq.NoCount, objResInfo.LocalPath, pSelf.CompleteCount, pSelf.TaskCount)

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

	if strings.Contains(objResInfo.URI, "shsz_idx_by_date") {
		log.Println("ExtractResData2............, ", objResInfo.Status, objResInfo.URI, objResInfo.SeqNo)
	}
}

/**
 * @brief		带重试功能的资源文件下载线程
 * @param[in]	sDataType 		资源文件类型
 * @param[in]	sUri			资源URI标识
 * @param[in]	sMD5			资源文件MD5校验码
 * @param[in]	sDateTime		服务端资源文件生成时间
 * @param[in]	nSeqNo			本次下载任务的任务编号
 * @param[in]	objParallelDownloadChannel	下载任务的同步管理
 * @param[in]	nRetryTimes		下载失败重试的最大次数
 * @note		如果多次重试下载后，还是失败，就是中断程序!
 */
func (pSelf *FileSyncClient) StartDataSafetyDownloader(sDataType, sUri, sMD5, sDateTime string, nSeqNo int, objParallelDownloadChannel chan int, objResFileChannel chan DownloadStatus, nRetryTimes int) {
	for n := 0; n < nRetryTimes; n++ {
		if strings.Contains(sUri, "shsz_idx_by_date") {
			log.Println("download............1, ", n, sUri, nSeqNo)
		}

		if nTaskStatus, sLocalPath := pSelf.FetchResource(sDataType, sUri, sMD5, sDateTime); nTaskStatus != ST_Error {
			if strings.Contains(sUri, "shsz_idx_by_date") {
				log.Println("download............2, ", n, sUri, nSeqNo)
			}
			pSelf.objCacheTable.NewResource(sUri, sLocalPath, nSeqNo)
			if strings.Contains(sUri, "shsz_idx_by_date") {
				log.Println("download............3, ", n, sUri, nSeqNo)
			}

			for bLoop := true; true == bLoop; {
				pSelf.objSeqLock.Lock()

				if objDataSeq, ok := pSelf.objMapDataSeq[sDataType]; ok {
					if (objDataSeq.LastSeqNo + 1) < nSeqNo {
						time.Sleep(time.Second)
						if strings.Contains(sUri, "shsz_idx_by_date") {
							log.Println("download............4, ", n, sUri, objDataSeq.LastSeqNo, nSeqNo)
						}
					} else {
						bLoop = false
						if nTaskStatus == ST_Completed {
							log.Printf("[INF] FileSyncClient.StartDataSafetyDownloader() : [√] %s:%d->%d => %s (Running:%d)", sDataType, objDataSeq.LastSeqNo, nSeqNo, sUri, len(objParallelDownloadChannel))
						} else if nTaskStatus == ST_Ignore {
							log.Printf("[INF] FileSyncClient.StartDataSafetyDownloader() : [Ignore] %s:%d->%d => %s (Running:%d)", sDataType, objDataSeq.LastSeqNo, nSeqNo, sUri, len(objParallelDownloadChannel))
						} else if nTaskStatus == ST_Error {
							log.Printf("[WARN] FileSyncClient.StartDataSafetyDownloader() : [×] %s:%d->%d Deleting File: => %s (Running:%d)", sDataType, objDataSeq.LastSeqNo, nSeqNo, sUri, len(objParallelDownloadChannel))
							os.Remove(sLocalPath)
						}

						objDataSeq.UncompressFlag = true
						pSelf.objMapDataSeq[sDataType] = objDataSeq
					}
				}

				pSelf.objSeqLock.Unlock()
			}

			if strings.Contains(sUri, "shsz_idx_by_date") {
				log.Println("download............5, ", n, sUri, nSeqNo)
			}
			<-objParallelDownloadChannel
			if strings.Contains(sUri, "shsz_idx_by_date") {
				log.Println("download............6, ", n, sUri, nSeqNo)
			}
			objResFileChannel <- DownloadStatus{MD5: sMD5, UPDATE: sDateTime, DataType: sDataType, URI: sUri, Status: nTaskStatus, LocalPath: sLocalPath, SeqNo: nSeqNo} // Mission Finished!
			if strings.Contains(sUri, "shsz_idx_by_date") {
				log.Println("download............7, ", n, sUri, nSeqNo)
			}

			return
		} else {
			pSelf.objCacheTable.NewResource(sUri, sLocalPath, nSeqNo)
			log.Printf("[WARN] FileSyncClient.StartDataSafetyDownloader() : failed 2 download, [RetryTimes=%d] %s:%d => %s", n+1, sDataType, nSeqNo, sUri)
			time.Sleep(time.Second * 1)
		}
	}

	pSelf.objCacheTable.IsNeedRollback = true
	pSelf.objCacheTable.RollbackUnextractedCacheFilesAndExit()
}

///< ----------------------------- NET: 资源操作/下载封装 ---------------------------------------------------
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
			Timeout:       0.1 * 60 * time.Second,
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
 * @brief		当前下载进度存盘更新
 * @param[in]	nAddRef		进度值，正负偏移量
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
