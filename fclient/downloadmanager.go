/**
 * @brief		下载引擎
 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
	"log"
	"os"
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

///////////////////////////////////// 下载资源的缓存文件管理类 //////////////////////

/**
 * @Class 		I_CacheFile
 * @brief		缓存文件管理接口
 * @author		barry
 */
type I_CacheFile interface {
	/**
	 * @brief		新增下载资源描述项
	 * @param[in]	sUri			资源URI标识串
	 * @param[in]	sFilePath		下载的资源本地路径
	 * @param[in]	nSeqNo			资源下载任务序号
	 */
	NewResource(sUri, sFilePath string, nSeqNo int)

	/**
	 * @brief		标记为资源文件“已经解压”
	 * @param[in]	sUri		资源URI标识串
	 */
	MarkExtractedRes(sUri string)

	/**
	 * @brief		资源删除、回滚 + 退出当前程序
	 * @note 		当IsNeedRollback标识被设置为true时，进行本地缓存文件回滚
	 */
	RollbackUnextractedCacheFilesAndExit()

	/**
	 * @brief		手动设计回滚标识
	 */
	SetRollbackFlag()
}

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
 * @brief		手动设计回滚标识
 */
func (pSelf *CacheFileTable) SetRollbackFlag() {
	pSelf.IsNeedRollback = true
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

///////////////////////////////////// 下载任务管理类 /////////////////////////////

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
 * @Class 		DownloadTask
 * @brief		资源下载任务及相关描述类
 * @author		barry
 */
type DownloadTask struct {
	LastSeqNo               int                 // Last Sequence No
	NoCount                 int                 // Number Of Resource
	UncompressFlag          bool                // Undo Flag
	TTL             		int                 // Time To Live
	RetryTimes      		int                 // 某资源下载失败重试最大次数
	ParallelDownloadChannel chan int            // 下载任务栈(用来控制最大并发数)
	ResFileChannel          chan DownloadStatus // 解压任务线
	I_Client				I_FClient			// 同步进度写盘接口
	I_CacheMgr				I_CacheFile			// 缓存文件管理接口
}

/**
 * @brief		某一类资源文件（列表）的下载器
 * @param[in]	sDataType 		资源类型
 * @param[in]	sTargetFolder	资源解压根目录
 * @param[in]	lstDownloadTask	这一类资源文件的下载清单表
 * @note 		给每个下载任务标一个时序号，然后解压的时候，就按这个顺序来一个一个的解压(保证该类别内资源文件的解压顺序)
 */
func (pSelf *DownloadTask) DownloadResourcesByCategory(sDataType string, sTargetFolder string, lstDownloadTask []ResDownload) {
	var nExtractedFileNum int = 0 // 在本资源文件类别中，已经解压文件的数量
	/////////////////////////// 在该资源类别下，建立分派下载任务 //////////////////////////
	for i, objRes := range lstDownloadTask {
		if strings.Contains(objRes.URI, "shsz_idx_by_date") {
			log.Println("extract............1, enter loop, ", objRes.URI, nExtractedFileNum, len(lstDownloadTask), objRes.TYPE, sDataType)
		}
		/////////////// 申请下载任务栈的一个占用名额 ///////////////
		pSelf.ParallelDownloadChannel <- i
		/////////////// 以同步有序的方式启动下线线程 ///////////////
		go pSelf.StartDataSafetyDownloader(objRes.TYPE, objRes.URI, objRes.MD5, objRes.UPDATE, i, pSelf.ParallelDownloadChannel, pSelf.ResFileChannel, pSelf.RetryTimes)
		////////////////////////// 等待有序的执行该类别中资源的解压任务 /////////////////////
		if strings.Contains(objRes.URI, "shsz_idx_by_date") {
			log.Println("extract............3, enter loop, ", objRes.URI, nExtractedFileNum, len(lstDownloadTask))
		}
		for j := 0; j < pSelf.TTL && nExtractedFileNum < len(lstDownloadTask); {
			select {
			case objStatus := <-pSelf.ResFileChannel:
				if strings.Contains(objStatus.URI, "shsz_idx_by_date") {
					log.Println("extract, grap a task............4, ", objStatus.Status, objStatus.URI, objStatus.SeqNo)
				}

				if objStatus.Status == ST_Completed { // 增量文件，需要解压
					pSelf.ExtractResData(sTargetFolder, objStatus)
					nExtractedFileNum += 1
				}

				if objStatus.Status == ST_Ignore { // 存量文件，只需忽略
					pSelf.I_Client.dumpProgress(1)
					pSelf.LastSeqNo = objStatus.SeqNo
					pSelf.UncompressFlag = false
				}

				if objStatus.Status == ST_Error {
					log.Println("[WARN] FileSyncServer.DownloadResourcesByCategory() : error in downloading :", objRes.URI)
				}
			default:
				if (len(pSelf.ParallelDownloadChannel) + len(pSelf.ResFileChannel)) == 0 {
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
func (pSelf *DownloadTask) ExtractResData(sTargetFolder string, objResInfo DownloadStatus) {
	if strings.Contains(objResInfo.URI, "shsz_idx_by_date") {
		log.Println("ExtractResData1............, ", objResInfo.Status, objResInfo.URI, objResInfo.SeqNo)
	}

	for {
		if (pSelf.LastSeqNo + 1) < objResInfo.SeqNo {
			log.Println("ExtractResData () : ..............................., ", objResInfo.URI, pSelf.LastSeqNo, objResInfo.SeqNo)
			time.Sleep(time.Second)
		} else {
			pSelf.I_CacheMgr.MarkExtractedRes(objResInfo.URI)
			///////////// Uncompress Resource File ///////////////////////////
			objUnzip := Uncompress{TargetFolder: sTargetFolder}
			if false == objUnzip.Unzip(objResInfo.LocalPath, objResInfo.URI) {
				os.Remove(objResInfo.LocalPath)
				log.Println("[ERROR] FileSyncClient.ExtractResData() :  error in uncompression : ", objResInfo.LocalPath)
				os.Exit(-100)
				return
			}

			pSelf.I_Client.dumpProgress(1)
			log.Printf("[INF] FileSyncClient.ExtractResData() : [DONE] [%s, %d-->%d] -----------> %s", objResInfo.DataType, objResInfo.SeqNo, pSelf.NoCount, objResInfo.LocalPath)
			pSelf.LastSeqNo = objResInfo.SeqNo
			pSelf.UncompressFlag = false
			break
		}
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
func (pSelf *DownloadTask) StartDataSafetyDownloader(sDataType, sUri, sMD5, sDateTime string, nSeqNo int, objParallelDownloadChannel chan int, objResFileChannel chan DownloadStatus, nRetryTimes int) {
	for n := 0; n < nRetryTimes; n++ {
		if strings.Contains(sUri, "shsz_idx_by_date") {
			log.Println("download............1, ", n, sUri, nSeqNo)
		}

		if nTaskStatus, sLocalPath := pSelf.I_Client.FetchResource(sDataType, sUri, sMD5, sDateTime); nTaskStatus != ST_Error {
			if strings.Contains(sUri, "shsz_idx_by_date") {
				log.Println("download............2, ", n, sUri, nSeqNo)
			}
			pSelf.I_CacheMgr.NewResource(sUri, sLocalPath, nSeqNo)
			if strings.Contains(sUri, "shsz_idx_by_date") {
				log.Println("download............3, ", n, sUri, nSeqNo)
			}

			for bLoop := true; true == bLoop; {
				if (pSelf.LastSeqNo + 1) < nSeqNo {
					time.Sleep(time.Second)
					if strings.Contains(sUri, "shsz_idx_by_date") {
						log.Println("download............4, ", n, sUri, pSelf.LastSeqNo, nSeqNo)
					}
				} else {
					bLoop = false
					if nTaskStatus == ST_Completed {
						log.Printf("[INF] FileSyncClient.StartDataSafetyDownloader() : [√] %s:%d->%d => %s (Running:%d)", sDataType, pSelf.LastSeqNo, nSeqNo, sUri, len(objParallelDownloadChannel))
					} else if nTaskStatus == ST_Ignore {
						log.Printf("[INF] FileSyncClient.StartDataSafetyDownloader() : [Ignore] %s:%d->%d => %s (Running:%d)", sDataType, pSelf.LastSeqNo, nSeqNo, sUri, len(objParallelDownloadChannel))
					} else if nTaskStatus == ST_Error {
						log.Printf("[WARN] FileSyncClient.StartDataSafetyDownloader() : [×] %s:%d->%d Deleting File: => %s (Running:%d)", sDataType, pSelf.LastSeqNo, nSeqNo, sUri, len(objParallelDownloadChannel))
						os.Remove(sLocalPath)
					}

					pSelf.UncompressFlag = true
				}
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
			pSelf.I_CacheMgr.NewResource(sUri, sLocalPath, nSeqNo)
			log.Printf("[WARN] FileSyncClient.StartDataSafetyDownloader() : failed 2 download, [RetryTimes=%d] %s:%d => %s", n+1, sDataType, nSeqNo, sUri)
			time.Sleep(time.Second * 1)
		}
	}

	pSelf.I_CacheMgr.SetRollbackFlag()
	pSelf.I_CacheMgr.RollbackUnextractedCacheFilesAndExit()
}
