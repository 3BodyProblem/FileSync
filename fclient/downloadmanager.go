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
	"runtime"
	"sync"
	"time"
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

//////////////////////////////////////// 下载任务管理类 /////////////////////////////

const (
	ST_Actived      TaskStatusType = iota // 任务状态0: 任务激活
	ST_Initializing                       // 任务状态1: 初始化中
	ST_Completed                          // 任务状态2: 下载完成
	ST_Ignore                             // 任务状态3: 不用下载
	ST_Error                              // 任务状态4: 任务出错
)

type TaskStatusType int // 任务类型描述值

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
	LastSeqNo               int                 // 最后一次下载且解压完成的任务编号(SeqNo)
	NoCount                 int                 // 本资源分类下的下载任务总数量
	TTL                     int                 // Time To Live
	RetryTimes              int                 // 某资源下载失败重试最大次数
	ParallelDownloadChannel chan int            // 下载任务栈(用来控制最大并发数)
	ResFileChannel          chan DownloadStatus // 解压任务线
	I_Downloader            I_Downloader        // 下载管理器接口
	I_CacheMgr              I_CacheFile         // 缓存文件管理接口
}

///< ---------------------- [Public 方法] -----------------------------
/**
 * @brief		某一类资源文件（列表）的下载器
 * @param[in]	sDataType 		资源类型
 * @param[in]	sTargetFolder	资源解压根目录
 * @param[in]	lstDownloadTask	这一类资源文件的下载清单表
 * @note 		给每个下载任务标一个时序号，然后解压的时候，就按这个顺序来一个一个的解压(保证该类别内资源文件的解压顺序)
 */
func (pSelf *DownloadTask) DownloadResourcesByCategory(sDataType string, sTargetFolder string, lstDownloadTask []ResDownload) {
	var nExtractedFileNum int = 0 // 在本资源文件类别中，已经解压文件的数量
	///// 跳过已经下载过的任务，若下载过的任务表中间有“脏数据”则清空这个类型的资源后全新下载 /////
	_, nDownloadIndex := pSelf.ClearInvalidHistorayCacheAndData(sTargetFolder, lstDownloadTask)
	if nDownloadIndex > 0 {
		pSelf.I_Downloader.DumpProgress(nDownloadIndex)
		log.Printf("[INF] FileSyncClient.DownloadResourcesByCategory() : [Ignore] DataType=%s, FileCount=%d", sDataType, nDownloadIndex)
	}
	/////////////////////////// 在该资源类别下，建立分派下载任务 //////////////////////////
	if len(lstDownloadTask[nDownloadIndex:]) > 0 {
		go pSelf.DownloadTaskDispatch(lstDownloadTask[nDownloadIndex:])
	}

	////////////////////////// 等待有序的执行该类别中资源的解压任务 /////////////////////
	for j := 0; j < pSelf.TTL && nExtractedFileNum < (len(lstDownloadTask)-nDownloadIndex); {
		select {
		case objStatus := <-pSelf.ResFileChannel: // 试着从解压任务栈，取一个解压任务
			if objStatus.Status == ST_Completed { // 增量文件，需要解压
				pSelf.ExtractResData(sTargetFolder, objStatus)
				nExtractedFileNum += 1
				pSelf.LastSeqNo = objStatus.SeqNo  // 更新最后一个完成的下载/解压任务的任务序号
				pSelf.I_Downloader.DumpProgress(1) // 存盘当前任务进度
			}

			if objStatus.Status == ST_Ignore { // 存量文件，只需忽略
				nExtractedFileNum += 1
				pSelf.LastSeqNo = objStatus.SeqNo
				pSelf.I_Downloader.DumpProgress(1)
			}

			if objStatus.Status == ST_Error {
				log.Println("[WARN] FileSyncServer.DownloadResourcesByCategory() : an error occur in downloading :", objStatus.URI)
				os.Exit(-200)
			}
		default: // 没有解压任务时，判断是继续等待还是中断循环
			if (len(pSelf.ParallelDownloadChannel)+len(pSelf.ResFileChannel)) == 0 && j > 60 {
				j = pSelf.TTL + 10
			}
			time.Sleep(time.Second)
		}
	}

	runtime.GC()
	log.Printf("[INF] FileSyncClient.DownloadResourcesByCategory() : [Release Downloader] %s : CompleteCount = %d, TotalCount = %d", sDataType, nExtractedFileNum, len(lstDownloadTask))
}

///< ---------------------- [Pivate 方法] -----------------------------
/**
 * @brief		如果历史缓存和数据中，只允许末尾有连续且未下载的资源文件，如果中间断也出现有不一致的资源文件则被视为“脏数据”，需要删除光该分类下的数据后做全新下载
 * @param[in]	lstDownloadTask		下载任务列表
 * @return		false,出错全清; true,返回待下载资源开始的位置索引
 * @note		要么发现出现在中间位置（历史位置）的“脏数据”出错全清，要么返回待下载资源开始的位置索引
 */
func (pSelf *DownloadTask) ClearInvalidHistorayCacheAndData(sTargetFolder string, lstDownloadTask []ResDownload) (bool, int) {
	var bIsIdentical bool = false		 // 服务器资源文件和本地缓存是否一致的标识
	var bHaveDiscrepancy bool = false    // 是否有不一致的缓存文件
	var nDisableIndexOfFirstTime int = 0 // 第一处不一致的位置索引

	for i, objRes := range lstDownloadTask {
		var objFCompare FComparison = FComparison{TargetFolder: sTargetFolder, URI: objRes.URI, MD5: objRes.MD5, DateTime: objRes.UPDATE} // 待下载资源与本地缓存文件的差异比较对象

		bIsIdentical, _ = objFCompare.Compare()
		if true == bIsIdentical {
			if true == bHaveDiscrepancy { // 在已经下载的资源中，如果发现中间位置有“脏资源”，需要清空该分类下的所有缓存和文件
				objFCompare.ClearCacheFolder()
				objFCompare.ClearDataFolder()
				return false, 0
			}

			continue
		}

		if false == bIsIdentical && false == bHaveDiscrepancy { // 找到第一处不一致的地方
			bHaveDiscrepancy = true
			nDisableIndexOfFirstTime = i
		}
	}

	// 特殊处理任务列表长度为1的资源列表
	if 1 == len(lstDownloadTask) {
		if false == bIsIdentical {
			return true, 0 // 有资源文件和缓存文件不一致，需要重新下载
		} else {
			return true, 1 // 不需要下载，都一致
		}
	}

	// 资源列表长度>1的情况
	if false == bHaveDiscrepancy {
		return true, len(lstDownloadTask) // 未找到不配对的情况
	} else {
		return true, nDisableIndexOfFirstTime // 找到在末尾几天有未下载的情况
	}
}

/**
 * @brief		把参数的下载任务列表里的任务全部发派出去
 * @param[in]	lstDownloadTask		下载任务列表
 * @note		保证下载并发数量不超过任务栈的长度限定
 */
func (pSelf *DownloadTask) DownloadTaskDispatch(lstDownloadTask []ResDownload) {
	for i, objRes := range lstDownloadTask {
		/////////////// 申请下载任务栈的一个占用名额 ///////////////
		pSelf.ParallelDownloadChannel <- i
		/////////////// 以同步有序的方式启动下线线程 ///////////////
		go pSelf.StartDataSafetyDownloader(objRes.TYPE, objRes.URI, objRes.MD5, objRes.UPDATE, i, pSelf.ParallelDownloadChannel, pSelf.ResFileChannel, pSelf.RetryTimes)
	}
}

/**
 * @brief		对本地下载的资源文件进行解压
 * @param[in]	sTargetFolder		下载资源文件解压的根目录
 * @param[in]	objResInfo			下载资源文件位置描述信息
 */
func (pSelf *DownloadTask) ExtractResData(sTargetFolder string, objResInfo DownloadStatus) {
	for {
		if (pSelf.LastSeqNo + 1) < objResInfo.SeqNo { // 在前一序号的任务未解压完成前，本任务的解压动作需要等待
			time.Sleep(time.Second)
		} else { // 需要解压当前的下载的资源文件
			pSelf.I_CacheMgr.MarkExtractedRes(objResInfo.URI)
			///////////// 解压下载的资源文件 ///////////////////////////
			objUnzip := Uncompress{TargetFolder: sTargetFolder}
			if false == objUnzip.Unzip(objResInfo.LocalPath, objResInfo.URI) {
				os.Remove(objResInfo.LocalPath)
				log.Println("[ERROR] FileSyncClient.ExtractResData() :  error in uncompression : ", objResInfo.LocalPath)
				os.Exit(-100)
				return
			}

			log.Printf("[INF] FileSyncClient.ExtractResData() : [DONE] [%s:%.1f%%, seq:%d-->last:%d] ---> %s", objResInfo.DataType, pSelf.I_Downloader.GetPercentageOfTasks(), objResInfo.SeqNo, pSelf.NoCount, objResInfo.URI)
			break
		}
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
	for n := 0; n < nRetryTimes; n++ { // 资源下载、解压（带任务的失败重试尝试循环）
		if nTaskStatus, sLocalPath := pSelf.I_Downloader.FetchResource(sDataType, sUri, sMD5, sDateTime); nTaskStatus != ST_Error {
			pSelf.I_CacheMgr.NewResource(sUri, sLocalPath, nSeqNo)

			for {
				if (pSelf.LastSeqNo + 1) < nSeqNo {
					time.Sleep(time.Second)
				} else {
					if nTaskStatus == ST_Completed {
						log.Printf("[INF] FileSyncClient.StartDataSafetyDownloader() : [√] %s:%.1f%%, %d->%d => %s (Running:%d)", sDataType, pSelf.I_Downloader.GetPercentageOfTasks(), pSelf.LastSeqNo, nSeqNo, sUri, len(objParallelDownloadChannel))
					} else if nTaskStatus == ST_Ignore {
						log.Printf("[INF] FileSyncClient.StartDataSafetyDownloader() : [Ignore] %s:%.1f%%, %d->%d => %s (Running:%d)", sDataType, pSelf.I_Downloader.GetPercentageOfTasks(), pSelf.LastSeqNo, nSeqNo, sUri, len(objParallelDownloadChannel))
					} else if nTaskStatus == ST_Error {
						log.Printf("[WARN] FileSyncClient.StartDataSafetyDownloader() : [×] %s:%.1f%%, %d->%d Deleting File: => %s (Running:%d)", sDataType, pSelf.I_Downloader.GetPercentageOfTasks(), pSelf.LastSeqNo, nSeqNo, sUri, len(objParallelDownloadChannel))
						os.Remove(sLocalPath)
					}
					break
				}
			}

			<-objParallelDownloadChannel                                                                                                                                 // 从下载任务栈，空出一个下载名额
			objResFileChannel <- DownloadStatus{MD5: sMD5, UPDATE: sDateTime, DataType: sDataType, URI: sUri, Status: nTaskStatus, LocalPath: sLocalPath, SeqNo: nSeqNo} // 将下载的资源文件描述，压入解压任务栈
			return
		} else {
			pSelf.I_CacheMgr.NewResource(sUri, sLocalPath, nSeqNo)
			log.Printf("[ERR] FileSyncClient.StartDataSafetyDownloader() : [×]-[ReloadTimes=%d] %s:%d->%d => %s", n+1, sDataType, pSelf.LastSeqNo, nSeqNo, sUri)
			time.Sleep(time.Second * 3)
		}
	}

	pSelf.I_CacheMgr.SetRollbackFlag()                      // 多次下载尝试失败，设置回滚标识
	pSelf.I_CacheMgr.RollbackUnextractedCacheFilesAndExit() // 回滚下载但未解压的缓存资源文件，退出本程序
}
