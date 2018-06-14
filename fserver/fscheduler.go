/**
 * @brief		Engine Of Server
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

func init() {
}

///////////////////////////////////// Comparison Stucture/Class
type RangeStruct struct {
	StartVal int
	EndVal   int
}

func (pSelf *RangeStruct) Fill(sFirstVal string, sSecVal string) bool {
	nNum1, err := strconv.Atoi(sFirstVal)
	if nil != err {
		log.Println("[ERROR] RangeStruct.Fill() : invalid number string: ", sFirstVal, err.Error())
		return false
	}

	nNum2, err := strconv.Atoi(sSecVal)
	if nil != err {
		log.Println("[ERROR] RangeStruct.Fill() : invalid number string: ", sSecVal, err.Error())
		return false
	}

	pSelf.StartVal = Min(nNum1, nNum2)
	pSelf.EndVal = Max(nNum1, nNum2)

	return true
}

type I_Range_OP interface {
	CodeInRange(sCodeNum string) bool
}

type RangeClass []RangeStruct

func (pSelf *RangeClass) CodeInRange(sCodeNum string) bool {
	nCodeNum, err := strconv.Atoi(sCodeNum)
	if nil != err {
		log.Println("[ERR] RangeClass.CodeInRange() : code is not digital: ", sCodeNum)
		return false
	}

	for _, objRange := range *pSelf {
		if nCodeNum >= objRange.StartVal && nCodeNum <= objRange.EndVal {
			return true
		}
	}

	//log.Println("[INF] RangeClass.CodeInRange() : ignore code : ", sCodeNum)
	return false
}

///////////////////////////////////// Configuration Stucture/Class
type DataSourceConfig struct {
	MkID       string    // market id ( SSE:shanghai SZSE:shenzheng )
	Folder     string    // data file folder
	MD5        string    // md5 of file
	UpdateTime time.Time // updatetime of file
}

///////////////////////////////////// File Scheduler Stucture/Class
type FileScheduler struct {
	XmlCfgPath       string                      // Xml Configuration File Path
	SyncFolder       string                      // Sync File Folder
	SZRealM1Folder   string                      // Folder Of SZ real data of minute1
	SHRealM1Folder   string                      // Folder Of SH real data of minute1
	DataSourceConfig map[string]DataSourceConfig // Data Source Config Of Markets
	BuildTime        int                         // Resources' Build Time
	RefSyncSvr       *FileSyncServer             // File SyncSvr Pointer
	codeRangeOfSH    RangeClass                  // Shanghai Code Range
	codeRangeOfSZ    RangeClass                  // Shenzheng Code Range
}

///////////////////////////////////// [OutterMethod]

func (pSelf *FileScheduler) GetRangeOP(sExchangeID string) I_Range_OP {
	var objRangeOp I_Range_OP = nil

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

//  Active File Scheduler
func (pSelf *FileScheduler) Active() bool {
	log.Println("[INF] FileScheduler.Active() : configuration file path: ", pSelf.XmlCfgPath)
	// Definition Of Profile's Structure
	var objCfg struct {
		XMLName xml.Name `xml:"cfg"`
		Version string   `xml:"version,attr"`
		Setting []struct {
			XMLName xml.Name `xml:"setting"`
			Name    string   `xml:"name,attr"`
			Value   string   `xml:"value,attr"`
		} `xml:"setting"`
	}
	///////////////////////////// Analyze configuration(.xml) 4 Engine ////////////////////////////
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
	/////////////////////////// Extract Settings ///////////////////////////////////////////////////
	log.Println("[INF] FileScheduler.Active() : [Xml.Setting] configuration file version: ", objCfg.Version)
	pSelf.DataSourceConfig = make(map[string]DataSourceConfig)
	for _, objSetting := range objCfg.Setting {
		switch strings.ToLower(objSetting.Name) {
		case "buildtime":
			pSelf.BuildTime, _ = strconv.Atoi(objSetting.Value)
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting] Build Time: ", pSelf.BuildTime)
		case "syncfolder":
			pSelf.SyncFolder = strings.Replace(objSetting.Value, "\\", "/", -1)
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting] SyncFolder: ", pSelf.SyncFolder)
		case "sse.real_m1":
			pSelf.SHRealM1Folder = strings.Replace(objSetting.Value, "\\", "/", -1)
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting] Real Data Folder(SH/M1): ", pSelf.SHRealM1Folder)
		case "szse.real_m1":
			pSelf.SZRealM1Folder = strings.Replace(objSetting.Value, "\\", "/", -1)
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting] Real Data Folder(SZ/M1): ", pSelf.SZRealM1Folder)
		case "sse.coderange":
			var objRange RangeStruct
			lstRangeStr := strings.Split(objSetting.Value, "~")
			objRange.Fill(lstRangeStr[0], lstRangeStr[1])
			pSelf.codeRangeOfSH = append(pSelf.codeRangeOfSH, objRange)
			log.Printf("[INF] FileScheduler.Active() : [Xml.Setting] SSE.coderange: [%d ~ %d]", objRange.StartVal, objRange.EndVal)
		case "szse.coderange":
			var objRange RangeStruct
			lstRangeStr := strings.Split(objSetting.Value, "~")
			objRange.Fill(lstRangeStr[0], lstRangeStr[1])
			pSelf.codeRangeOfSZ = append(pSelf.codeRangeOfSZ, objRange)
			log.Printf("[INF] FileScheduler.Active() : [Xml.Setting] SZSE.coderange: [%d ~ %d]", objRange.StartVal, objRange.EndVal)
		default:
			sResType := strings.ToLower(objSetting.Name)
			if len(strings.Split(objSetting.Name, ".")) <= 1 {
				log.Println("[WARNING] FileScheduler.Active() : [Xml.Setting] Ignore -> ", objSetting.Name)
				continue
			}

			objSetting.Value = strings.Replace(objSetting.Value, "\\", "/", -1)
			pSelf.DataSourceConfig[sResType] = DataSourceConfig{MkID: strings.ToLower(strings.Split(objSetting.Name, ".")[0]), Folder: objSetting.Value}
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting]", sResType, pSelf.DataSourceConfig[sResType].MkID, pSelf.DataSourceConfig[sResType].Folder)
		}
	}
	/////////////////////////// First Time 2 Build Resources
	if false == pSelf.CompressSyncResource("") {
		return false
	}
	if false == pSelf.RefSyncSvr.LoadResList() {
		return false
	}

	log.Println("[INF] FileScheduler.CompressSyncResource() : [OK] Resources List Builded! ......")
	go pSelf.ResRebuilder()

	return true
}

func (pSelf *FileScheduler) ResRebuilder() {
	for i := 0; i < 999; i++ {
		time.Sleep(time.Second * 15)                          // Sleep 4 a while
		pSelf.CompressSyncResource("")                        // Judge whether 2 compress quotation files
		if true == SyncQLFtpFilesInPeriodTime(64000, 65000) { // Sync qiulong ftp resource files (HKSE)
			pSelf.CompressSyncResource("HKSE")
			time.Sleep(time.Second * 60 * 2)
		}

		if true == SyncQLFtpFilesInPeriodTime(90500, 91000) { // Sync qiulong ftp resource files (SSE/SZSE)
			pSelf.CompressSyncResource("HKSE")
			time.Sleep(time.Second * 60 * 2)
		}

		if i%(4*5) == 0 {
			i = 0
			pSelf.RebuildRealMinute1() // Rebuild Real Data of Minute 1 Line
		}
	}
}

///////////////////////////////////// [InnerMethod]
func (pSelf *FileScheduler) CompressSyncResource(sSpecifyResType string) bool {
	sSpecifyResType = strings.ToLower(sSpecifyResType)
	objNowTime := time.Now()
	objBuildTime := time.Date(objNowTime.Year(), objNowTime.Month(), objNowTime.Day(), pSelf.BuildTime/10000, pSelf.BuildTime/100%100, pSelf.BuildTime%100, 0, time.Local)

	/////////////////////////////// Judge Whether 2 Compress Quotation Files
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

	/////////////////////////////// Judge Whether 2 Compress A New Resoures(.tar.gz) Or Not
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
		log.Printf("[INF] FileScheduler.CompressSyncResource() : (BuildTime=%s) Building Sync Resources ......", time.Now().Format("2006-01-02 15:04:05"))
		/////////////////////// iterate data source configuration && compress quotation files ////////
		for sResType, objDataSrcCfg := range pSelf.DataSourceConfig {
			sDataType := strings.ToLower(sResType[:strings.Index(sResType, ".")])
			if "" == sSpecifyResType || sDataType == sSpecifyResType {
				lstRes, bIsOk := objCompressor.XCompress(sResType, &objDataSrcCfg, pSelf.GetRangeOP(sResType))
				if true == bIsOk {
					/////////////// record resource path && MD5 which has been compressed
					objNewResList.Download = append(objNewResList.Download, lstRes...)
					log.Println("[INF] FileScheduler.CompressSyncResource() : [OK] TarFile : ", objDataSrcCfg.Folder)
				} else {
					log.Println("[WARN] FileScheduler.CompressSyncResource() : [FAILURE] TarFile : ", objDataSrcCfg.Folder)
					return false
				}
			}
		}

		if "" == sSpecifyResType { //// Unspecify Resource Type
			//////////////////////// Save status 2 ./status.dat /////////////////////////////////////
			objStatusSaver, err := os.Create("./status.dat")
			if nil != err {
				log.Println("[ERROR] FileScheduler.CompressSyncResource() : [FAILURE] cannot save ./status.dat 2 disk :", err.Error())
			} else {
				objStatusSaver.Write([]byte(objNowTime.Format("2006-01-02 15:04:05")))
				objStatusSaver.Close()
			}
			/////////////////////// Set rebuild data 2 Response obj. ////////////////////////////////
			pSelf.RefSyncSvr.SetResList(&objNewResList)
			log.Println("[INF] FileScheduler.CompressSyncResource() : [OK] Sync Resources(All) Builded! ......")
		} else { /////////////////////// Specify Resource Type
			pSelf.RefSyncSvr.UpdateResList(&objNewResList)
			log.Printf("[INF] FileScheduler.CompressSyncResource() : [OK] Sync Resources(SpecifyType) Builded! Count = %d......", len(objNewResList.Download))
		}
	}

	return true
}

func (pSelf *FileScheduler) RebuildRealMinute1() {
	var objToday time.Time = time.Now()
	var nToday int = objToday.Year()*10000 + int(objToday.Month())*100 + objToday.Day()
	var nNowT int = objToday.Hour()*10000 + objToday.Minute()*100 + objToday.Second()
	var bInRebuildPeriod bool = false

	if (nNowT >= 93000 && nNowT <= 153000) || (pSelf.RefSyncSvr.GetSHRealMin1File() == "" || pSelf.RefSyncSvr.GetSZRealMin1File() == "") {
		bInRebuildPeriod = true
	}

	if len(pSelf.SHRealM1Folder) > 0 && true == bInRebuildPeriod { // minute 1 lines of shanghai
		var nRetTime int = objToday.Hour()*100 + objToday.Minute()
		var objCompressor Compressor = Compressor{TargetFolder: pSelf.SyncFolder}
		var objDataSrcCfg = DataSourceConfig{MkID: "sse", Folder: pSelf.SHRealM1Folder}

		_, bIsOk := objCompressor.XCompress("sse.real_m1", &objDataSrcCfg, pSelf.GetRangeOP("sse."))
		if true == bIsOk {
			log.Println("[INF] FileScheduler.RebuildRealMinute1() : [OK] TarFile : ", objDataSrcCfg.Folder)
			sSrcFile := fmt.Sprintf("%s%d", filepath.Join(pSelf.SyncFolder, "SSE/MIN1_TODAY/MIN1_TODAY."), nToday)
			sSrcFile = strings.Replace(sSrcFile, "\\", "/", -1)
			sDestFile := fmt.Sprintf("%s.%d", sSrcFile, nRetTime)

			err := os.Rename(sSrcFile, sDestFile)
			if err != nil {
				log.Println("[WARN] FileScheduler.RebuildRealMinute1() : [ERROR] cannot rename file : ", sSrcFile)
			} else {
				pSelf.RefSyncSvr.SetSHRealMin1File(sDestFile)
			}
		} else {
			log.Println("[WARN] FileScheduler.RebuildRealMinute1() : [FAILURE] TarFile : ", objDataSrcCfg.Folder)
		}
	}

	if len(pSelf.SZRealM1Folder) > 0 && true == bInRebuildPeriod { // minute 1 lines of shenzheng
		var nRetTime int = objToday.Hour()*100 + objToday.Minute()
		var objCompressor Compressor = Compressor{TargetFolder: pSelf.SyncFolder}
		var objDataSrcCfg = DataSourceConfig{MkID: "szse", Folder: pSelf.SZRealM1Folder}

		_, bIsOk := objCompressor.XCompress("szse.real_m1", &objDataSrcCfg, pSelf.GetRangeOP("szse."))
		if true == bIsOk {
			log.Println("[INF] FileScheduler.RebuildRealMinute1() : [OK] TarFile : ", objDataSrcCfg.Folder)
			sSrcFile := fmt.Sprintf("%s%d", filepath.Join(pSelf.SyncFolder, "SZSE/MIN1_TODAY/MIN1_TODAY."), nToday)
			sSrcFile = strings.Replace(sSrcFile, "\\", "/", -1)
			sDestFile := fmt.Sprintf("%s.%d", sSrcFile, nRetTime)

			err := os.Rename(sSrcFile, sDestFile)
			if err != nil {
				log.Println("[WARN] FileScheduler.RebuildRealMinute1() : [ERROR] cannot rename file : ", sSrcFile)
			} else {
				pSelf.RefSyncSvr.SetSZRealMin1File(sDestFile)
			}
		} else {
			log.Println("[WARN] FileScheduler.RebuildRealMinute1() : [FAILURE] TarFile : ", objDataSrcCfg.Folder)
		}
	}
}
