/**
 * @brief		判断是否为新合并出来的资源压缩包:

 				新合并出来的资源压缩包定义：
 					a) 因往年的资源文件是按年份进行压缩的，今年的文件是按月份和天两种时间单位进行分包压缩的，所以在刚刚跨年后一天，先前因为当今年的原因被分散到各月/日小文件中的数据会被合并生成到一个新的“年”文件中（成为历史年份数据文件）
 					b) 因当年内的资源文件是按月/日为压缩单为存文件的，所以中间存在着不久前的日文件，因时间的推移，被包含进新的月文件的可能，这也是新合并出来的资源包的一种

 				对新合并出来的资源压包的处理：
 					只需要下载到缓存，不做解压处理（因为这些数据之前在增量下载中已经解压了），不然会干扰已经存在的数据

				对这类资源文件的判定方法：
					a) New年资源文件：
						给定ResourceFile年份,存在该年份中最后一天行情数据 ===> 就标记为只需下载即可 （判断该年尾月最后一天是否存在数据）
					b) New月资源文件：
						给定ResourceFile月份,存在访月份中最后一天行情数据 ===> 就标记为只需下载即可 （判断指定月的最后一天是否存在数据）

 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
 	"log"
 	"path"
 	"path/filepath"
 	"strings"
)

var (
	GlobalCombinationFileJudgement   CombinationFileJudgement 	// 全局“只下载文件”判定记录器
)

//////////////////// 判断某新合并出来的资源文件是否为只需要下载

/**
 * @Class 		DownloadOnlyFile
 * @brief		某个只需下载不用解压的缓存资源文件描述类
 * @author		barry
 */
type DownloadOnlyFile struct {
	TYPE 		 string // 资源类型，如： sse.d1 / szse.m60 ...
	URI          string // 资源URI路径
}

/**
 * @Class 		CombinationFileJudgement
 * @brief		判断并记录那些只需下载，不需要解压的资源文件的信息
 * @author		barry
 */
type CombinationFileJudgement struct {
	objDownloadOnlyFileTable map[string]DownloadOnlyFile // 只需下载，不用解压的资源清单表
}

///< ---------------------- [Public 方法] -----------------------------
/**
 * @bruef		初始化
 */
func (pSelf *CombinationFileJudgement) Initialize() bool {
	pSelf.objDownloadOnlyFileTable = make(map[string]DownloadOnlyFile)

	return true
}

/**
 * @brief		判断某资源文件是否是只需下载不用解压的 新合并资源文件
 * @param[in]	resFile 		待下载资源信息
 * @param[in]	sCacheFolder	缓存根目录
 * @return		true			是新合并的资源文件，只要下载即可
 				false			需要下载后，再解压
 * @note		对于判定为只需要下载的文件，会被记录到 “只下载” 列表中
 */
func (pSelf *CombinationFileJudgement) JudgeAndRecord( resFile *ResDownload, sCacheFolder string ) bool {
	sLocalFolder, err := filepath.Abs((filepath.Dir("./")))
	if err != nil {
		log.Println("[WARN] CombinationFileJudgement.JudgeAndRecord() : failed 2 fetch absolute path of program")
		return true
	}

	sLocalFolder = filepath.Join(sLocalFolder, sCacheFolder)	// 生成资源文件缓存的根目录
	sLocalFile := filepath.Join(sLocalFolder, resFile.URI)		// 生成某资源文件包的路径
	sLocalFolder = path.Dir(sLocalFile)							// 生成该资源文件包的所在目录
	sMkID := strings.Split(resFile.TYPE, ".")[0]
	sDataType := strings.Split(resFile.TYPE, ".")[1]
	log.Printf("[INF] CombinationFileJudgement.JudgeAndRecord() : Judging Resource -> (%s|%s) %s", sMkID, sDataType, sLocalFile)

	return true
}
