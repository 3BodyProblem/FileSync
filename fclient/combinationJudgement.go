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
	"io/ioutil"
	"log"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	GlobalCombinationFileJudgement CombinationFileJudgement // 全局“只下载文件”判定记录器
)

//////////////////// 判断某新合并出来的资源文件是否为只需要下载

/**
 * @Class 		DownloadOnlyFile
 * @brief		某个只需下载不用解压的缓存资源文件描述类
 * @author		barry
 */
type DownloadOnlyFile struct {
	TYPE string // 资源类型，如： sse.d1 / szse.m60 ...
	URI  string // 资源URI路径
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
func (pSelf *CombinationFileJudgement) JudgeDownloadOnly(resFile *ResDownload, sCacheFolder string) bool {
	sLocalFolder, _ := filepath.Abs((filepath.Dir("./")))
	sLocalFolder = filepath.Join(sLocalFolder, sCacheFolder)      // 生成缓存的根目录
	sLocalFolder = filepath.Join(sLocalFolder, resFile.URI)       // 生成缓存的Resources File目录
	sLocalFolder = path.Dir(sLocalFolder)                         // 生成缓存的Resources File Root目录
	sLoadFile := filepath.Join(sLocalFolder, resFile.TYPE+".txt") // 生成某类型数据文件的最后日期备案文件的路径
	sMkID := strings.Split(resFile.TYPE, ".")[0]
	sDataType := strings.Split(resFile.TYPE, ".")[1]

	//////////////// 沪、深市场的日线、60分钟线的比较策略 定义如下: ///////////////////////////
	if (sMkID == "sse" || sMkID == "szse") && (sDataType == "d1" || sDataType == "m60") {
		sExpiredDate, _ := ioutil.ReadFile(sLoadFile)                    // 某数据类型的数据的写盘的最后日期string
		nFileDate, _ := strconv.Atoi(strings.Split(resFile.URI, ".")[1]) // 资源文件代表的数据日期(部分数据文件可能只代表到年，所以后面的月和日都为0,最后几天代表日数据的文件精确到月/日)
		nExpiredDate, _ := strconv.Atoi(string(sExpiredDate))            // 某数据类型的数据的写盘的最后日期

		if nFileDate > 0 && nExpiredDate > 0 { // 最后几天代表日数据的文件精确到月/日
			var objToday time.Time = time.Now()
			/////////////// 如果文件名是"前一年"的情况： 数据文件日期只代表到年，所以后面的月和日都为0 --> 需要取到该年最后一个工作日的日期后进行比较
			if nFileDate%100 == 0 && (objToday.Year()-1) == nFileDate/10000 {
				objDateOfLastYear := time.Date(objToday.Year()/10000, time.Month(100), 1, 8, 1, 2, 0, time.Local)
				for {
					dDiff, _ := time.ParseDuration("-24h")
					objDateOfLastYear = objDateOfLastYear.Add(dDiff)
					if objDateOfLastYear.Weekday().String() != "Saturday" && objDateOfLastYear.Weekday().String() != "Sunday" {
						break
					} else {
						continue
					}
				}

				if objDateOfLastYear.Year()*10000 == nFileDate { // 新合并生成的去年的资源包，因为文件名不含月/日，需要补足这块信息后再比较
					nFileDate = objDateOfLastYear.Year()*10000 + int(objDateOfLastYear.Month())*100 + objDateOfLastYear.Day()
					log.Printf("[INF] CombinationFileJudgement.JudgeDownloadOnly() : New Merged File Of Last Year (%s) %s, Gen File Date = %d", objDateOfLastYear.Format("2006-01-02 15:04:05"), objDateOfLastYear.Weekday().String(), nFileDate)
				}
			}

			//////////////// 如果文件名是月/日文件的情况： //////////////////////////
			objFileDate := time.Date(nFileDate/10000, time.Month(nFileDate%10000/100), nFileDate%100, 21, 6, 9, 0, time.Local)
			subHours := objToday.Sub(objFileDate)
			nDays := subHours.Hours() / 24
			if nDays > 32 {
				return false // 早于当前日一个月的数据文件，即要下载，又要解压
			}
			/////////////////////////////////////////////////////////////////////
			if nFileDate > nExpiredDate {
				log.Printf("[INF] CombinationFileJudgement.JudgeDownloadOnly() : Merged Resource Download Only -> (%s|%s)%s FileDate = %d, ExpiredDate = %d", sMkID, sDataType, resFile.URI, nFileDate, nExpiredDate)
				objDownloadOnlyFileInfo := DownloadOnlyFile{TYPE: resFile.TYPE, URI: resFile.URI}
				pSelf.objDownloadOnlyFileTable[resFile.URI] = objDownloadOnlyFileInfo
				return true
			} else {
				log.Printf("[INF] CombinationFileJudgement.JudgeDownloadOnly() : Merged Resource Download&Extract -> (%s|%s)%s FileDate = %d, ExpiredDate = %d", sMkID, sDataType, resFile.URI, nFileDate, nExpiredDate)
				return false
			}
		}

		return false // 比较条件不全，即下载，又解压
	}

	return false // 不属于指定判断类型的数据，即下载，又解压
}

/**
 * @brief		根据传入文件数据信息（文件名中的日期、文件类型等）存盘压解出的最后数据的日期，用于以后判断“新合并”数据是否需要被下载或者解压
 * @param[in]	resFile 		下载的缓存文件信息
 * @param[in]	sCacheFolder	缓存根目录
 * @return		true			保存日期成功
 */
func (pSelf *CombinationFileJudgement) RecordExpiredDate4DataType(resFile *DownloadStatus, sCacheFolder string) bool {
	sLocalFolder, err := filepath.Abs((filepath.Dir("./")))
	if err != nil {
		log.Println("[ERR] CombinationFileJudgement.RecordExpiredDate4Data() : failed 2 fetch absolute path of program")
		return true
	}

	sLocalFolder = filepath.Join(sLocalFolder, sCacheFolder)          // 生成缓存的根目录
	sLocalFolder = filepath.Join(sLocalFolder, resFile.URI)           // 生成缓存的Resources File目录
	sLocalFolder = path.Dir(sLocalFolder)                             // 生成缓存的Resources File Root目录
	sDumpFile := filepath.Join(sLocalFolder, resFile.DataType+".txt") // 生成某类型数据文件的最后日期备案文件的路径
	sMkID := strings.Split(resFile.DataType, ".")[0]                  // 市场代码
	sDataType := strings.Split(resFile.DataType, ".")[1]              // 数据类型码

	if (sMkID == "sse" || sMkID == "szse") && (sDataType == "d1" || sDataType == "m60") {
		sExpiredDate := strings.Split(resFile.URI, ".")[1]             // 某数据类型的数据的写盘的最后日期
		err := ioutil.WriteFile(sDumpFile, []byte(sExpiredDate), 0644) // 存盘最后一个数据文件的日期
		if nil != err {
			log.Println("[ERR] CombinationFileJudgement.RecordExpiredDate4Data() : failed dump expired date for datetype: ", resFile.DataType)
			return false
		}
	}

	return true
}
