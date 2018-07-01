/**
 * @brief		资源文件压缩
 * @detail 		需要在函数XCompress（）中配置针对不同资源类型的压缩策略
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"archive/tar"
	"compress/zlib"
	"crypto/md5"
	"fmt"
	//"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

//////////////////////////////////// 资源压缩写盘句柄管理类 ///////////////////////////////////////
/**
 * @class		CompressHandles
 * @brief		目标文件句丙管理
 * @author		barry
 */
type CompressHandles struct {
	TarFile    *os.File     // 目标文件句柄
	GZipWriter *zlib.Writer // 目标zlib.Writer句柄
	TarWriter  *tar.Writer  // 目标tar.Writer 句柄
}

/**
* @brief		目标文件打开
* @param[in]	sFilePath				目标文件路径
* @param[in]	nZlibpCompressLevel		zlib的压缩级别 (缺省为： zlib.DefaultCompression级别)
* @return		true					打开成功
				false					打开失败
*/
func (pSelf *CompressHandles) OpenFile(sFilePath string, nZlibpCompressLevel int) bool {
	var err error

	pSelf.TarFile, err = os.Create(sFilePath)
	if err != nil {
		log.Println("[ERR] CompressHandles.OpenFile() : failed 2 create *.tar file :", sFilePath, err.Error())
		return false
	}

	pSelf.GZipWriter, err = zlib.NewWriterLevel(pSelf.TarFile, nZlibpCompressLevel)
	if err != nil {
		log.Println("[ERR] CompressHandles.OpenFile() : failed 2 create *tar.Writer :", sFilePath, err.Error())
		return false
	}

	pSelf.TarWriter = tar.NewWriter(pSelf.GZipWriter)
	if err != nil {
		log.Println("[ERR] CompressHandles.OpenFile() : failed 2 create *gzip.Writer :", sFilePath, err.Error())
		return false
	}

	return true
}

/**
 * @brief		关闭目标文件句柄
 */
func (pSelf *CompressHandles) CloseFile() {
	if pSelf.GZipWriter != nil {
		pSelf.GZipWriter.Close()
	}
	if pSelf.TarWriter != nil {
		pSelf.TarWriter.Close()
	}
	if pSelf.TarFile != nil {
		pSelf.TarFile.Close()
	}
}

///////////////////////////////// 资源源文件中，每中资源的 提取+压缩 策略类 //////////////////////////////////
/**
 * @class		BaseRecordIO
 * @brief		每中资源的 提取+压缩 策略基类
 * @author		barry
 */
type BaseRecordIO struct {
	DataType        string                     // 资源文件所属类型
	CodeRangeFilter I_CodeRange_Filter         // 资源文件对应市场的有效代码段
	mapFileHandle   map[string]CompressHandles // 资源文件压缩过程中，根据文件句缓存对应的文件句柄(提高性能)
}

/**
* @brief		初始化文件句柄
* @return		true			成功
				false			失败
*/
func (pSelf *BaseRecordIO) Initialize() bool {
	pSelf.mapFileHandle = make(map[string]CompressHandles)
	return true
}

/**
 * @brief		释放并关闭文件句柄（缓存中）
 * @detail 		内部进行文件资源列表的 时间顺序 返回
 * @return 		按 "时间顺序"，返回带 md5校验码 的压缩后资源 文件路径 及相关信息
 * @note 		函数按文件路径排序，日期靠前的文件在前面，有助于生成的“资源列表”是有时序性的，方便解压的时候，按时间顺序恢复行情数据
 */
func (pSelf *BaseRecordIO) Release() []ResDownload {
	var lstRes []ResDownload
	var lstSortKeys []string
	log.Println("[INF] BaseRecordIO.Release() : flushing files 2 disk, count =", len(pSelf.mapFileHandle))
	// 对文件路径，按时间顺序，制作资源列表
	for sPath, objHandles := range pSelf.mapFileHandle {
		objHandles.CloseFile()
		lstSortKeys = append(lstSortKeys, sPath)
	}

	// 输出的资源列表排序： 必须是时间序，这个时间序将会下发给同步client.exe依赖
	sort.Strings(lstSortKeys)
	// 按时间遍历，并提取MD5串
	for _, sVal := range lstSortKeys {
		data, err := ioutil.ReadFile(sVal)
		//objMd5File, err := os.Open(sVal)
		if err != nil {
			log.Println("[WARN] BaseRecordIO.Release() : local file is not exist :", sVal)
			return lstRes
		}

		/////////////////////// Generate MD5 String
		/*objMD5Hash := md5.New()
		if _, err := io.Copy(objMD5Hash, objMd5File); err != nil {
			log.Printf("[WARN] BaseRecordIO.Release() : failed 2 generate MD5 : %s : %s", sVal, err.Error())
			objMd5File.Close()
			return lstRes
		}

		objMd5File.Close()*/
		sMD5 := strings.ToLower(fmt.Sprintf("%x" /*objMD5Hash*/, md5.Sum(data)))
		log.Printf("[INF] BaseRecordIO.Release() : close file = %s, md5 = %s", sVal, sMD5)
		lstRes = append(lstRes, ResDownload{TYPE: pSelf.DataType, URI: sVal, MD5: sMD5, UPDATE: time.Now().Format("2006-01-02 15:04:05")})
	}

	// 返回带 时间序 的资源列表
	return lstRes
}

/**
 * @brief		默认压缩级别设定
 */
func (pSelf *BaseRecordIO) GetCompressLevel() int {
	return zlib.DefaultCompression
}

/**
 * @brief		默认不过滤任何商品代码
 */
func (pSelf *BaseRecordIO) CodeInWhiteTable(sFileName string) bool {
	return true
}

/**
* @brief		默认直接使用从数据源目录获取的文件路径
* @param[in]	sFileName			每个源资源文件的递归路径
* @detail 		因为对某些需要根据原始数据进行计算/派生资源类型需要重新指定目录或目录句，多数情况下使用原始路径和文件句
				(比如，需要从 XXX/MIN1/目录计算的XXX/MIN60/就需要重新格式化并定向目标路径)
* @return		返回新的目标文件的递归路径
*/
func (pSelf *BaseRecordIO) GenFilePath(sFileName string) string {
	return sFileName
}

/**
* @brief		目标压缩文件路径编制 + 句柄返回函数
* @detail		先根据 目标文件前缀路径 + 源文件全路径 + 源文件数据记录日期(date)生成 ==> 目标缩压文件全路径
				再根据目标缩压文件全路径， 打开新的，或在缓存中配对出已经打开的文件句柄
* @param[in]	sFilePath		目标压缩文件的路径
* @param[in]	nDate 			从源数据文件读取记录的日期
* @param[in]	sSrcFile		源数据文件的路径
*/
func (pSelf *BaseRecordIO) GrapWriter(sFilePath string, nDate int, sSrcFile string) *tar.Writer {
	var sFile string = ""
	var objToday time.Time = time.Now()
	// 先计算源文件中，行情记录数据的年份是否为近期
	objRecordDate := time.Date(nDate/10000, time.Month(nDate%10000/100), nDate%100, 21, 6, 9, 0, time.Local)
	subHours := objToday.Sub(objRecordDate)
	nDays := subHours.Hours() / 24

	if nDays <= 16 { // 如果是近期，则目标压缩文件，一天的数据生成一个文件名(带全日期)
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate)
	} else { // 如果不是近期，则目标压缩文件，半个月的数据一个文件名(带上下月信息)
		nDD := (nDate % 100) ////////// One File With 2 Week's Data Inside
		if nDD <= 15 {
			nDD = 0
		} else {
			nDD = 15
		}
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate/100*100+nDD)
	}

	// 根据生成的目标文件句 打开、或从缓存中返回已经打开的目标压缩文件的句柄
	if objHandles, ok := pSelf.mapFileHandle[sFile]; ok {
		return objHandles.TarWriter
	} else {
		var objCompressHandles CompressHandles

		if true == objCompressHandles.OpenFile(sFile, pSelf.GetCompressLevel()) {
			pSelf.mapFileHandle[sFile] = objCompressHandles

			return pSelf.mapFileHandle[sFile].TarWriter
		} else {
			log.Println("[ERR] BaseRecordIO.GrapWriter() : failed 2 open *tar.Writer :", sFilePath)
		}

	}

	return nil
}

/**
 * @class		I_Record_IO
 * @brief		每中资源的 提取+压缩 策略基础接口
 * @author		barry
 */
type I_Record_IO interface {
	Initialize() bool
	Release() []ResDownload
	LoadFromFile(bytesData []byte) ([]byte, int, int)                    // load data from file, return [] byte (return nil means end of file)
	CodeInWhiteTable(sFileName string) bool                              // judge whether the file need 2 be loaded
	GenFilePath(sFileName string) string                                 // generate name  of file which in .tar
	GrapWriter(sFilePath string, nDate int, sSrcFile string) *tar.Writer // grap a .tar writer ptr
	GetCompressLevel() int                                               // get gzip compression level
}

////////////////////////////////////// 资源压缩总类 ////////////////////////////////////////////////
/**
 * @class		Compressor
 * @brief		能对源数据目录，进行递归读取+压缩
 * @detail 		以递归方式取出数据源 ---> 经在XCompress()中配置的策略 ---> 目标压缩文件
 * @author		barry
 */
type Compressor struct {
	TargetFolder string // 压缩后资源文件存放的根目录
}

///< ----------------------------- [Private 方法] ----------------------------------------
/**
* @brief		递归压缩目录
* @detail 		这个函数的功能主要就是递归
* @param[in]	sDestFile		目标文件路径和前缀
* @param[in]	sSrcFolder		数据源目录
* @param[in]	sRecursivePath	递归目录
* @param[in]	pILoader		记录策略类接口
* @return		true			成功
				false			失败
*/
func (pSelf *Compressor) compressFolder(sDestFile string, sSrcFolder string, sRecursivePath string, pILoader I_Record_IO) bool {
	oDirFile, err := os.Open(sSrcFolder) // Open source diretory
	if err != nil {
		log.Println("[INF] Compressor.compressFolder() : cannot open source folder :", sSrcFolder, err.Error())
		return false
	}
	defer oDirFile.Close()

	lstFileInfo, err := oDirFile.Readdir(0) // Get file info slice
	if err != nil {
		return false
	}

	for _, oFileInfo := range lstFileInfo {
		sCurPath := path.Join(sSrcFolder, oFileInfo.Name()) // Append path
		if oFileInfo.IsDir() {                              // Check it is directory or file
			pSelf.compressFolder(sDestFile, sCurPath, path.Join(sRecursivePath, oFileInfo.Name()), pILoader) // (Directory won't add unitl all subfiles are added)
		}

		compressFile(sDestFile, sCurPath, path.Join(sRecursivePath, oFileInfo.Name()), oFileInfo, pILoader)
	}

	return true
}

/**
 * @brief		文件压缩函数
 * @detail 		具体压缩策略的使用逻辑也在这里编码
 * @param[in]	sDestFile		目标文件存放路径（ Path => 目标根目录 + 市场编号 + 子目录 + 文件句前缀部分 ）
 * @param[in]	sSrcFile		源文件存放全路径
 * @param[in]	pILoader		在XCompress()中定义的数据 提取+压缩 策略接口
 * @note 		核心的引擎函数
 */
func compressFile(sDestFile string, sSrcFile string, sRecursivePath string, oFileInfo os.FileInfo, pILoader I_Record_IO) bool {
	if oFileInfo.IsDir() {
	} else {
		var nIndex int = 0   // 遍历过的文件数据的长度
		var nDataLen int = 0 // 读出的全部文件数据的总长度

		////////// 按源文件路径名过滤 ---> 按XCompress()中定义的，各类型资源的文件级过滤条件
		if pILoader.CodeInWhiteTable(sSrcFile) == false {
			return true
		}

		////////// 打开源文件 && 读出所有数据记录 ////////////////////
		oSrcFile, err := os.Open(sSrcFile)
		if err != nil {
			return false
		}
		defer oSrcFile.Close()
		bytesData, err := ioutil.ReadAll(oSrcFile)
		if err != nil {
			return false
		}

		///////// 遍历所有的数据记录 && 压缩目标文件 /////////////////
		nDataLen = len(bytesData)
		for nIndex < nDataLen {
			hdr := new(tar.Header) // Create tar header
			//hdr, err := tar.FileInfoHeader(oFileInfo, "")
			///// 遍历 && 全部或部分读出记录集
			hdr.Name = pILoader.GenFilePath(sRecursivePath)                    // 根据源文件的路径，按XCompress()中定义的策略，复用或生成新的目标文件路径
			bData, nDate, nOffset := pILoader.LoadFromFile(bytesData[nIndex:]) // 按XCompress()中定义的策略，批量返回部分或全部的 "重新格式化" 后的记录
			nIndex += nOffset

			if nDate < 19901010 || nDate > 20301010 { // 判断记录体中的无效日期
				continue
			}

			///// 根据目标文件路径和前缀 + 行情记录的日期 + 源文件路径，根据XCompress()中定义的策略, 打开或从缓存中返回对应目标的句柄
			pTarWriter := pILoader.GrapWriter(pILoader.GenFilePath(sDestFile), nDate, sSrcFile)
			if nil == pTarWriter {
				return false
			}

			hdr.Size = int64(len(bData))
			hdr.Mode = int64(oFileInfo.Mode())
			//< 注意： hdr.ModTime本应填被压文件修改时间，但为方便资源同步时的md5比较(因文件修改时间会被算在md5串中)，故使用固定时间
			hdr.ModTime = time.Date(2018, time.Month(1), 2, 21, 6, 9, 0, time.Local) //oFileInfo.ModTime()
			err = pTarWriter.WriteHeader(hdr)                                        // 写压缩数据头
			if err != nil {
				log.Println("[INF] Compressor.compressFile() : cannot write tar header 2 file :", sDestFile, err.Error())
				return false
			}

			pTarWriter.Write(bData) // 写数据体
		}
	}

	return true
}

///< ---------------------------- [Public 方法] --------------------------------------------------
/**
 * @brief		压缩函数及策略
 * @detail 		需要在这里定义各数据类型的压缩策略
 * @param[in]	sResType		资源类型
 * @param[in]	objDataSrc		资源的市场和存放路径
 * @param[in]	codeRange 		对应市场的有效代码过滤器
 * @return		有时间顺序的压缩后目标文件路径 + 带md5码 和 成功标识(true/false)
 */
func (pSelf *Compressor) XCompress(sResType string, objDataSrc *DataSourceConfig, codeRange I_CodeRange_Filter) ([]ResDownload, bool) {
	var lstRes []ResDownload                                                                     // 带时间顺序的目标资源文件路径
	var sDataType string = strings.ToLower(sResType[strings.Index(sResType, "."):])              // 数据类型 (d1/m1/m5/wt)
	var sDestFolder string = filepath.Join(pSelf.TargetFolder, strings.ToUpper(objDataSrc.MkID)) // 目标文件存放路径（ Path => 目标根目录 + 市场编号 ）
	log.Printf("[INF] Compressor.XCompress() : [Compressing] ExchangeCode:%s, DataType:%s, Folder:%s", objDataSrc.MkID, sDataType, objDataSrc.Folder)
	sDestFolder = strings.Replace(sDestFolder, "\\", "/", -1)
	// 压缩策略配置部分
	switch {
	case (objDataSrc.MkID == "sse" && sDataType == ".st") || (objDataSrc.MkID == "szse" && sDataType == ".st"):
		objRecordIO := StaticRecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of Weight data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "STATIC/STATIC."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".wt") || (objDataSrc.MkID == "szse" && sDataType == ".wt"):
		objRecordIO := WeightRecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of Weight data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "WEIGHT/WEIGHT."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".d1") || (objDataSrc.MkID == "szse" && sDataType == ".d1"):
		objRecordIO := Day1RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of Day data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "DAY/DAY."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".m1") || (objDataSrc.MkID == "szse" && sDataType == ".m1"):
		objRecordIO := Minutes1RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of M1 data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "MIN/MIN."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".real_m1") || (objDataSrc.MkID == "szse" && sDataType == ".real_m1"):
		objRecordIO := RealMinutes1RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of M1 data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "MIN1_TODAY/MIN1_TODAY."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".m5") || (objDataSrc.MkID == "szse" && sDataType == ".m5"):
		objRecordIO := Minutes5RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of M5 data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "MIN5/MIN5."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".m60") || (objDataSrc.MkID == "szse" && sDataType == ".m60"):
		objRecordIO := Minutes60RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of M60 data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "MIN60/MIN60."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".participant":
		objRecordIO := ParticipantRecordIO{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "Participant."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".shase_rzrq_by_date":
		objRecordIO := Shase_rzrq_by_date{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "shase_rzrq_by_date/shase_rzrq_by_date."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".sznse_rzrq_by_date":
		objRecordIO := Sznse_rzrq_by_date{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "sznse_rzrq_by_date/sznse_rzrq_by_date."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".shsz_idx_by_date":
		objRecordIO := Shsz_idx_by_date{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "shsz_idx_by_date/shsz_idx_by_date."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".shsz_detail":
		objRecordIO := Shsz_detail{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "shsz_detail/shsz_detail."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "qlfile" && sDataType == ".column_dy_bk":
		objRecordIO := DYColumnRecordIO{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "dybk."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "qlfile" && sDataType == ".column_gn_bk":
		objRecordIO := GNColumnRecordIO{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "gnbk."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "qlfile" && sDataType == ".column_hy_bk":
		objRecordIO := HYColumnRecordIO{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "hybk."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "qlfile" && sDataType == ".column_zs_bk":
		objRecordIO := ZSColumnRecordIO{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "zsbk."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "qlfile" && sDataType == ".blockinfo_ini":
		objRecordIO := BlkInfoRecordIO{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "blkinfo."), objDataSrc.Folder, &objRecordIO)
	default:
		log.Printf("[ERR] Compressor.XCompress() : [Compressing] invalid exchange code(%s) or data type(%s)", objDataSrc.MkID, sDataType)
		return lstRes, false
	}
}

/**
 * @brief		某一资源类型压缩函数
 * @detail 		遍历某类资源文件的根目录，压根据策略，提取压缩出新的目标数据文件
 * @param[in]	sDestFile		目标文件存放路径（ Path => 目标根目录 + 市场编号 + 子目录 + 文件句前缀部分 ）
 * @param[in]	sSrcFolder		源文件存放Root目录
 * @return		有时间顺序的压缩后目标文件路径 + 带md5码 和 成功标识(true/false)
 */
func (pSelf *Compressor) TranslateFolder(sDestFile, sSrcFolder string, pILoader I_Record_IO) ([]ResDownload, bool) {
	var lstRes []ResDownload                   // 带时间顺序的目标资源文件路径
	var sMkFolder string = path.Dir(sDestFile) // 截取出需要事先创建好的目标文件的目录树

	///////////////// 准备好目标目录树
	if "windows" == runtime.GOOS {
		sMkFolder = sDestFile[:strings.LastIndex(sDestFile, "\\")]
	}
	sDestFile = strings.Replace(sDestFile, "\\", "/", -1)

	err := os.MkdirAll(sMkFolder, 0755) // 创建目录
	if err != nil {
		log.Println("[ERR] Compressor.TranslateFolder() : cannot build target folder 4 zip file :", sMkFolder)
		return lstRes, false
	}

	//////////////// 初始化数据提取、压缩策略对象
	log.Printf("[INF] Compressor.TranslateFolder() : compressing ---> (%s)", sSrcFolder)
	if false == pILoader.Initialize() {
		log.Println("[ERR] Compressor.TranslateFolder() : Cannot initialize I_Record_IO object, ", sSrcFolder)
		return lstRes, false
	}

	//////////////// 遍历源头目录，提取、压缩目标文件
	sDestFile = pILoader.GenFilePath(sDestFile)
	if "windows" != runtime.GOOS {
		if false == pSelf.compressFolder(sDestFile, sSrcFolder, path.Base(sSrcFolder), pILoader) {
			return lstRes, false
		}
	} else {
		if false == pSelf.compressFolder(sDestFile, sSrcFolder, "./", pILoader) {
			return lstRes, false
		}
	}

	/////////////// 关闭目标文件，且返回有时间顺序的压缩后目标文件路径 + 带md5码 和 成功标识(true/false)
	return pILoader.Release(), true // 注：文件句丙关闭的后，根据目标文件路径的日期进行排序
}
