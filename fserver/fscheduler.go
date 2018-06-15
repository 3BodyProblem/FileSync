/**
* @brief		行情资源文件生成服务
* @detail		资源文件分类：
				1) 历史资源：
					1.1) 主要资源： 行情资源(日线、分钟线等)，定时在午夜开始生成(只包含今天前的数据)
					1.2) 附加资源： 扩展资源（钱龙板块分类信息）, 定时在午夜开始生成
					1.3) FTP资源：  由台湾合作方提供的资源，开启独立小程序从ftp同步后，由本程序生成压缩文件（每天在几个配置时段做多次同步，以防同步时异常）
				2) 实时资源:
					沪、深今天内的实时1分钟线，每过n分钟生成一次；供quoteclientapi下载
* @author		barry
* @date		2018/4/10
*/
package fserver

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	//"runtime/pprof"
	"strconv"
	"strings"
	"time"
)

///////////////////////////////////// 商品代码范围限定类 //////////////////////////////////////
/**
 * @class 		CodeRangeStruct
 * @brief		限定某市场某类代码的区段
 * @note 		不在区段内的代码，将不参与资源压缩的过程
 * @author		barry
 */
type CodeRangeStruct struct {
	StartVal int // 有效代码段开始值
	EndVal   int // 有效代码段结束值
}

/**
 * @brief		设置代码段起始结束值
 * @param[in]	sFirstVal		起始代码
 * @param[in]	sSecVal			结束代码
 */
func (pSelf *CodeRangeStruct) Fill(sFirstVal string, sSecVal string) bool {
	nNum1, err := strconv.Atoi(sFirstVal)
	if nil != err {
		log.Println("[ERROR] CodeRangeStruct.Fill() : invalid number string: ", sFirstVal, err.Error())
		return false
	}

	nNum2, err := strconv.Atoi(sSecVal)
	if nil != err {
		log.Println("[ERROR] CodeRangeStruct.Fill() : invalid number string: ", sSecVal, err.Error())
		return false
	}

	pSelf.StartVal = Min(nNum1, nNum2)
	pSelf.EndVal = Max(nNum1, nNum2)

	return true
}

/**
 * @Class 		CodeRangeClass
 * @brief		某市场所有有效代码区段的列表类
 * @author		barry
 */
type CodeRangeClass []CodeRangeStruct

/**
 * @brief		判断某代码是否为在限定的代码区段内
 * @note 		不在区段内的代码，将不参与资源压缩的过程
 */
func (pSelf *CodeRangeClass) CodeInRange(sCodeNum string) bool {
	nCodeNum, err := strconv.Atoi(sCodeNum)
	if nil != err {
		log.Println("[ERR] CodeRangeClass.CodeInRange() : code is not digital: ", sCodeNum)
		return false
	}

	for _, objRange := range *pSelf {
		if nCodeNum >= objRange.StartVal && nCodeNum <= objRange.EndVal {
			return true
		}
	}

	//log.Println("[INF] CodeRangeClass.CodeInRange() : ignore code : ", sCodeNum)
	return false
}

/**
 * @Interface	I_CodeRange_Filter
 * @brief		针对某市场： 判断某代码是否在代码段中
 * @note 		不在区段内的代码，将不参与资源压缩的过程
 * @author		barry
 */
type I_CodeRange_Filter interface {
	/**
	 * @brief	判断某代码是否为在限定的代码区段内
	 * @note 	不在区段内的代码，将不参与资源压缩的过程
	 */
	CodeInRange(sCodeNum string) bool
}

///////////////////////////////////// 行情资源生成服务类 //////////////////////////////////////
/**
 * @Class 		DataSourceConfig
 * @brief		从配置文件(xml)等加载的待压缩资源的路径和相关信息
 * @author		barry
 */
type DataSourceConfig struct {
	MkID   string // 市场编号 ( SSE:上海 SZSE:深圳 )
	Folder string // 待压缩的资源文件所在目录（比如：D:\HQHISDATA\SSE\MIN\ 和 D:\HQHISDATA\SSE\DAY\ )
}

/**
 * @Class 		FileScheduler
 * @brief		资源生成服务
 * @detail 		生成历史资源 + 实时资源
 * @author		barry
 */
type FileScheduler struct {
	XmlCfgPath     string                      // xml配置文件路径（由命令行启动参数输入）
	SyncFolder     string                      // 待生成的资源文件所在根目录（由命令行启动参数输入）
	SZRealM1Folder string                      // 待生成的深圳今天内的实时1分钟线根目录（实时：每过n分钟生成一次）
	SHRealM1Folder string                      // 待生成的上海今天内的实时1分钟线根目录（实时：每过n分钟生成一次）
	DataSrcCfg     map[string]DataSourceConfig // 待生成的各历史行情资源所在根目录（历史：定时生成）
	BuildTime      int                         // 历史行情资源生成操作激活时间(分钟线、日线、权息信息等)
	RefSyncSvr     *FileSyncServer             // 资源下载网络服务器引用对象
	codeRangeOfSH  CodeRangeClass              // 上海市场：需要参与资源包生成的有效代码段
	codeRangeOfSZ  CodeRangeClass              // 深圳市场：需要参与资源包生成的有效代码段
}

///< ---------------------------- [Public 方法] --------------------------------------------------
/**
 * @brief		根据交易所代码，获取对应的商品代码过滤方法对象
 * @param[in]	sExchangeID		交易所代码
 * @return		商品代码过滤接口 (I_CodeRange_Filter)
 */
func (pSelf *FileScheduler) GetCodeRangeFilter(sExchangeID string) I_CodeRange_Filter {
	var objRangeOp I_CodeRange_Filter = nil

	sExchangeID = strings.ToLower(sExchangeID)
	if strings.Index(sExchangeID, "sse") >= 0 {
		if len(pSelf.codeRangeOfSH) == 0 {
			return nil
		}

		objRangeOp = &pSelf.codeRangeOfSH
	}

	if strings.Index(sExchangeID, "szse") >= 0 {
		if len(pSelf.codeRangeOfSZ) == 0 {
			return nil
		}

		objRangeOp = &pSelf.codeRangeOfSZ
	}

	return objRangeOp
}

/**
* @brief		激活启动资源文件生成服务
* @detail		1) 读取资源生成配置任务
				2) 做一次启动时的首次资源压缩（如果今天内未生成过）
				3) 启动资源压缩任务线程，用于每天自动更新压缩 历史、FTP、实时的资料数据
* @return		true		启动成功
				false		启动失败
*/
func (pSelf *FileScheduler) Active() bool {
	log.Println("[INF] FileScheduler.Active() : configuration file path: ", pSelf.XmlCfgPath)
	///////////////////////////// 加载本地配置文件(.xml)并初始化到结构中 ////////////////////////////
	var objCfg struct { // 定义本地xml配置文件的内存结构，用于加载服务配置项
		XMLName xml.Name `xml:"cfg"`
		Version string   `xml:"version,attr"`
		Setting []struct {
			XMLName xml.Name `xml:"setting"`
			Name    string   `xml:"name,attr"`
			Value   string   `xml:"value,attr"`
		} `xml:"setting"`
	}

	sXmlContent, err := ioutil.ReadFile(pSelf.XmlCfgPath)
	if err != nil {
		log.Println("[WARN] FileScheduler.Active() : cannot locate configuration file, path: ", pSelf.XmlCfgPath)
		return false
	}

	err = xml.Unmarshal(sXmlContent, &objCfg)
	if err != nil {
		log.Println("[WARN] FileScheduler.Active() : cannot parse xml configuration file, error: ", err.Error())
		return false
	}

	/////////////////////////// 遍历从xml配置中加载的objCfg结构，设定各参数 /////////////////////////////
	log.Println("[INF] FileScheduler.Active() : [Xml.Setting] configuration file version: ", objCfg.Version)
	pSelf.DataSrcCfg = make(map[string]DataSourceConfig)
	for _, objSetting := range objCfg.Setting {
		switch strings.ToLower(objSetting.Name) {
		case "buildtime": // 历史资源文件生成时间(日线、分钟线、权息信息等)
			pSelf.BuildTime, _ = strconv.Atoi(objSetting.Value)
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting] Build Time: ", pSelf.BuildTime)
		case "syncfolder": // 生成资源文件存在的根目录
			pSelf.SyncFolder = strings.Replace(objSetting.Value, "\\", "/", -1)
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting] SyncFolder: ", pSelf.SyncFolder)
		case "sse.real_m1": // 上海，实时1分钟线数据源存放目录
			pSelf.SHRealM1Folder = strings.Replace(objSetting.Value, "\\", "/", -1)
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting] Real Data Folder(SH/M1): ", pSelf.SHRealM1Folder)
		case "szse.real_m1": // 深圳，实时1分钟线数据源存放目录
			pSelf.SZRealM1Folder = strings.Replace(objSetting.Value, "\\", "/", -1)
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting] Real Data Folder(SZ/M1): ", pSelf.SZRealM1Folder)
		case "sse.coderange": // 上海，参与历史资源数据压缩的合法代码段设定
			var objRange CodeRangeStruct
			lstRangeStr := strings.Split(objSetting.Value, "~")
			objRange.Fill(lstRangeStr[0], lstRangeStr[1])
			pSelf.codeRangeOfSH = append(pSelf.codeRangeOfSH, objRange)
			log.Printf("[INF] FileScheduler.Active() : [Xml.Setting] SSE.coderange: [%d ~ %d]", objRange.StartVal, objRange.EndVal)
		case "szse.coderange": // 深圳，参与历史资源数据压缩的合法代码段设定
			var objRange CodeRangeStruct
			lstRangeStr := strings.Split(objSetting.Value, "~")
			objRange.Fill(lstRangeStr[0], lstRangeStr[1])
			pSelf.codeRangeOfSZ = append(pSelf.codeRangeOfSZ, objRange)
			log.Printf("[INF] FileScheduler.Active() : [Xml.Setting] SZSE.coderange: [%d ~ %d]", objRange.StartVal, objRange.EndVal)
		default: // 历史数据资源（非实时）部分的数据源存放目录及相关信息设定，并构建到资源源对象中(pSelf.DataSrcCfg)
			sResType := strings.ToLower(objSetting.Name) // 资源类型(如，SSE.m60 / SZSE.d1 / HKSE.shase_rzrq_by_date)
			if len(strings.Split(objSetting.Name, ".")) <= 1 {
				log.Println("[WARNING] FileScheduler.Active() : [Xml.Setting] Ignore -> ", objSetting.Name)
				continue
			}

			objSetting.Value = strings.Replace(objSetting.Value, "\\", "/", -1)
			pSelf.DataSrcCfg[sResType] = DataSourceConfig{MkID: strings.ToLower(strings.Split(objSetting.Name, ".")[0]), Folder: objSetting.Value}
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting]", sResType, pSelf.DataSrcCfg[sResType].MkID, pSelf.DataSrcCfg[sResType].Folder)
		}
	}

	///////////////////////////// 启动时先压缩一次待下载资源文件(若，今日内已经压缩过，则跳空) ////////////////////////////
	if false == pSelf.compressHistoryResource("") {
		return false
	}

	///////////////////////////// 加载资源列表xml字符串 + 并从xml中恢复出资源列表结构对象 ////////////////////////////////
	if false == pSelf.RefSyncSvr.LoadResList() {
		return false
	}

	log.Println("[INF] FileScheduler.compressHistoryResource() : [OK] Resources List Builded! ......")
	///////////////////////////// 启动成功， 开辅助线程： 定时FTP资源资源 + 实时资源 压缩线程 ////////////////////////////
	go pSelf.allResourcesRebuilderThread()

	return true
}

///< ----------------------------- [Private 方法] ----------------------------------------
/**
* @brief		资源自动生成线程函数
* @detail		历史资源 + 实时资源的生成线程：
				1) 历史部分： 午夜生成
				2) FTP资源定时生成: 定时生成
				3) 实时数据资源生成(比如，今日内的1分钟线): 盘中每n分钟生成一次
*/
func (pSelf *FileScheduler) allResourcesRebuilderThread() {
	for i := 0; i < 999; i++ {
		time.Sleep(time.Second * 15)                          // Sleep 4 a while
		pSelf.compressHistoryResource("")                     // 每天午夜生成一次历史数据
		if true == SyncQLFtpFilesInPeriodTime(64000, 65000) { // 定时从FTP下载资源
			pSelf.compressHistoryResource("HKSE") // 定时压缩从FTP下载的资源
			time.Sleep(time.Second * 60 * 2)
		}

		if true == SyncQLFtpFilesInPeriodTime(90500, 91000) { // 定时从FTP下载资源
			pSelf.compressHistoryResource("HKSE") // 定时压缩从FTP下载的资源
			time.Sleep(time.Second * 60 * 2)
		}

		if i%(4*5) == 0 {
			i = 0
			pSelf.rebuildRealMinute1() // 每n分钟压缩一次今日内的实时1分钟线资源
		}
	}
}

/**
* @brief		历史资料压缩函数
* @detail		压缩的数据内容有：
				1) 历史行情资源: 转码机生成的分钟线、日线、权息等
				2) 附加资源： 钱龙的板块信息等
				3) FTP资源： 由客户的FTP提供的内容
* @param[in]	sSpecifyResType			不为空串时， 表示： 不做全类型压缩，只压缩指定的资源类型
										空串，表示做全类型的资源压缩
* @return		true					成功
				false					失败
* @note 		当做全类型压缩时，需要判断今日是否已经做过压缩，如果已经做过，则跳空
*/
func (pSelf *FileScheduler) compressHistoryResource(sSpecifyResType string) bool {
	sSpecifyResType = strings.ToLower(sSpecifyResType)
	objNowTime := time.Now()
	objBuildTime := time.Date(objNowTime.Year(), objNowTime.Month(), objNowTime.Day(), pSelf.BuildTime/10000, pSelf.BuildTime/100%100, pSelf.BuildTime%100, 0, time.Local)
	////////////////////// 全类型资源的压缩，一天只做一次，做过就不用做了 //////////////////////
	if "" == sSpecifyResType {
		objStatusLoader, err := os.Open("./status.dat")
		defer objStatusLoader.Close()
		if nil == err {
			bytesData := make([]byte, 20)
			objStatusLoader.Read(bytesData)
			nYY, nMM, nDD, _, _, _, bIsOk := parseTimeStr(string(bytesData))
			if true == bIsOk {
				if objNowTime.Year() == nYY && int(objNowTime.Month()) == nMM && int(objNowTime.Day()) == nDD {
					return true
				}
			}
		}
	}

	//////////////////// 开始压缩指定资源 ///////////////////////////////////////////////////
	if objNowTime.After(objBuildTime) == true {
		/////// Performance Testing Code, as follow /////////////////
		/*f, err := os.Create("performace_test_compression.dat")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()*/
		/////////////////////////////////////////////////////////////
		var objNewResList ResourceList
		var objCompressor Compressor = Compressor{TargetFolder: pSelf.SyncFolder}
		log.Printf("[INF] FileScheduler.compressHistoryResource() : (BuildTime=%s) Building Sync Resources ......", time.Now().Format("2006-01-02 15:04:05"))
		/////////////////////// iterate data source configuration && compress quotation files ////////
		for sResType, objDataSrcCfg := range pSelf.DataSrcCfg {
			sDataType := strings.ToLower(sResType[:strings.Index(sResType, ".")])
			if "" == sSpecifyResType || sDataType == sSpecifyResType {
				lstRes, bIsOk := objCompressor.XCompress(sResType, &objDataSrcCfg, pSelf.GetCodeRangeFilter(sResType))
				if true == bIsOk {
					/////////////// record resource path && MD5 which has been compressed
					objNewResList.Download = append(objNewResList.Download, lstRes...)
					log.Println("[INF] FileScheduler.compressHistoryResource() : [OK] TarFile : ", objDataSrcCfg.Folder)
				} else {
					log.Println("[WARN] FileScheduler.compressHistoryResource() : [FAILURE] TarFile : ", objDataSrcCfg.Folder)
					return false
				}
			}
		}

		if "" == sSpecifyResType { //// 全类型资源压缩后： 更新资源列表 + 存盘记录更新日期
			//////////////////////// Save status 2 ./status.dat /////////////////////////////////////
			objStatusSaver, err := os.Create("./status.dat")
			if nil != err {
				log.Println("[ERROR] FileScheduler.compressHistoryResource() : [FAILURE] cannot save ./status.dat 2 disk :", err.Error())
			} else {
				objStatusSaver.Write([]byte(objNowTime.Format("2006-01-02 15:04:05")))
				objStatusSaver.Close()
			}
			/////////////////////// Set rebuild data 2 Response obj. ////////////////////////////////
			pSelf.RefSyncSvr.SetResList(&objNewResList)
			log.Println("[INF] FileScheduler.compressHistoryResource() : [OK] Sync Resources(All) Builded! ......")
		} else { /////////////////////// 指定类型压缩后： 只更新资源列表
			pSelf.RefSyncSvr.UpdateResList(&objNewResList)
			log.Printf("[INF] FileScheduler.compressHistoryResource() : [OK] Sync Resources(SpecifyType) Builded! Count = %d......", len(objNewResList.Download))
		}
	}

	return true
}

/**
 * @brief		沪深今日内的实时1分钟线压缩函数
 * @detail		只在 "盘中" 或者 "今日内实时数据尚未做过一次压缩" 的情况下，进行实时的压缩
 */
func (pSelf *FileScheduler) rebuildRealMinute1() {
	var objToday time.Time = time.Now()
	var nToday int = objToday.Year()*10000 + int(objToday.Month())*100 + objToday.Day()
	var nNowT int = objToday.Hour()*10000 + objToday.Minute()*100 + objToday.Second()
	var bInRebuildPeriod bool = false
	// 判断是否需要做压缩
	if (nNowT >= 93000 && nNowT <= 153000) || (pSelf.RefSyncSvr.GetSHRealMin1File() == "" || pSelf.RefSyncSvr.GetSZRealMin1File() == "") {
		bInRebuildPeriod = true
	}
	// 压缩今日上海1分钟线
	if len(pSelf.SHRealM1Folder) > 0 && true == bInRebuildPeriod { // minute 1 lines of shanghai
		var nRetTime int = objToday.Hour()*100 + objToday.Minute()
		var objCompressor Compressor = Compressor{TargetFolder: pSelf.SyncFolder}
		var objDataSrcCfg = DataSourceConfig{MkID: "sse", Folder: pSelf.SHRealM1Folder}

		_, bIsOk := objCompressor.XCompress("sse.real_m1", &objDataSrcCfg, pSelf.GetCodeRangeFilter("sse."))
		if true == bIsOk {
			log.Println("[INF] FileScheduler.rebuildRealMinute1() : [OK] TarFile : ", objDataSrcCfg.Folder)
			sSrcFile := fmt.Sprintf("%s%d", filepath.Join(pSelf.SyncFolder, "SSE/MIN1_TODAY/MIN1_TODAY."), nToday)
			sSrcFile = strings.Replace(sSrcFile, "\\", "/", -1)
			sDestFile := fmt.Sprintf("%s.%d", sSrcFile, nRetTime)

			err := os.Rename(sSrcFile, sDestFile)
			if err != nil {
				log.Println("[WARN] FileScheduler.rebuildRealMinute1() : [ERROR] cannot rename file : ", sSrcFile)
			} else {
				pSelf.RefSyncSvr.SetSHRealMin1File(sDestFile)
			}
		} else {
			log.Println("[WARN] FileScheduler.rebuildRealMinute1() : [FAILURE] TarFile : ", objDataSrcCfg.Folder)
		}
	}
	// 压缩今日深圳1分钟线
	if len(pSelf.SZRealM1Folder) > 0 && true == bInRebuildPeriod { // minute 1 lines of shenzheng
		var nRetTime int = objToday.Hour()*100 + objToday.Minute()
		var objCompressor Compressor = Compressor{TargetFolder: pSelf.SyncFolder}
		var objDataSrcCfg = DataSourceConfig{MkID: "szse", Folder: pSelf.SZRealM1Folder}

		_, bIsOk := objCompressor.XCompress("szse.real_m1", &objDataSrcCfg, pSelf.GetCodeRangeFilter("szse."))
		if true == bIsOk {
			log.Println("[INF] FileScheduler.rebuildRealMinute1() : [OK] TarFile : ", objDataSrcCfg.Folder)
			sSrcFile := fmt.Sprintf("%s%d", filepath.Join(pSelf.SyncFolder, "SZSE/MIN1_TODAY/MIN1_TODAY."), nToday)
			sSrcFile = strings.Replace(sSrcFile, "\\", "/", -1)
			sDestFile := fmt.Sprintf("%s.%d", sSrcFile, nRetTime)

			err := os.Rename(sSrcFile, sDestFile)
			if err != nil {
				log.Println("[WARN] FileScheduler.rebuildRealMinute1() : [ERROR] cannot rename file : ", sSrcFile)
			} else {
				pSelf.RefSyncSvr.SetSZRealMin1File(sDestFile)
			}
		} else {
			log.Println("[WARN] FileScheduler.rebuildRealMinute1() : [FAILURE] TarFile : ", objDataSrcCfg.Folder)
		}
	}
}
