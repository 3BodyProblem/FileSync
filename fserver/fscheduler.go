/**
 * @brief		Engine Of Server
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"encoding/xml"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func init() {
}

func Min(x, y int) int {
	if x < y {
		return x
	}

	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}

	return y
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
		log.Println("[ERROR] RangeClass.CodeInRange() : code is not digital: ", sCodeNum)
		return false
	}

	for _, objRange := range *pSelf {
		if nCodeNum >= objRange.StartVal && nCodeNum <= objRange.EndVal {
			return true
		}
	}

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
	DataSourceConfig map[string]DataSourceConfig // Data Source Config Of Markets
	BuildTime        int                         // Resources' Build Time
	LastUpdateTime   time.Time                   // Last Updatetime
	RefSyncSvr       *FileSyncServer             // File SyncSvr Pointer
	codeRangeOfSH    RangeClass                  // Shanghai Code Range
	codeRangeOfSZ    RangeClass                  // Shenzheng Code Range
}

///////////////////////////////////// [OutterMethod]
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

	///////////////////////////// Analyze configuration(.xml) 4 Engine
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

	/////////////////////////// Extract Settings
	log.Println("[INF] FileScheduler.Active() : [Xml.Setting] configuration file version: ", objCfg.Version)
	pSelf.LastUpdateTime = time.Now().AddDate(-1, 0, -1)
	pSelf.DataSourceConfig = make(map[string]DataSourceConfig)
	for _, objSetting := range objCfg.Setting {
		switch strings.ToLower(objSetting.Name) {
		case "buildtime":
			pSelf.BuildTime, _ = strconv.Atoi(objSetting.Value)
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting] Build Time: ", pSelf.BuildTime)
		case "syncfolder":
			pSelf.SyncFolder = strings.Replace(objSetting.Value, "\\", "/", -1)
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting] SyncFolder: ", pSelf.SyncFolder)
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
			sSetting := strings.ToLower(objSetting.Name)
			if len(strings.Split(objSetting.Name, ".")) <= 1 {
				log.Println("[WARNING] FileScheduler.Active() : [Xml.Setting] Ignore -> ", objSetting.Name)
				continue
			}

			objSetting.Value = strings.Replace(objSetting.Value, "\\", "/", -1)
			pSelf.DataSourceConfig[sSetting] = DataSourceConfig{MkID: strings.ToLower(strings.Split(objSetting.Name, ".")[0]), Folder: objSetting.Value}
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting] ", sSetting, pSelf.DataSourceConfig[sSetting].MkID, pSelf.DataSourceConfig[sSetting].Folder)
		}
	}

	/////////////////////////// First Time 2 Build Resources
	if true == pSelf.compressSyncResource() {
		return false
	}

	if false == pSelf.RefSyncSvr.LoadResList() {
		return false
	}

	pSelf.LastUpdateTime = time.Now() // update time
	log.Println("[INF] FileScheduler.compressSyncResource() : [OK] Resources List Builded! ......", pSelf.LastUpdateTime.Format("2006-01-02 15:04:05"))
	go pSelf.ResRebuilder()

	return true
}

func (pSelf *FileScheduler) ResRebuilder() {
	for {
		time.Sleep(time.Second * 15)
		objNowTime := time.Now()
		objStatusLoader, err := os.Open("./status.dat")
		defer objStatusLoader.Close()
		/////////////////////////////// Judge Whether 2 Compress Quotation Files
		if nil == err {
			bytesData := make([]byte, 20)
			objStatusLoader.Read(bytesData)
			nYY, nMM, nDD, _, _, _, bIsOk := parseTimeStr(string(bytesData))
			if true == bIsOk {
				if objNowTime.Year() == nYY && int(objNowTime.Month()) == nMM && int(objNowTime.Day()) == nDD {
					continue
				}

				nNowTime := objNowTime.Hour()*10000 + objNowTime.Minute()*100 + objNowTime.Second()
				if nNowTime > pSelf.BuildTime {
					log.Println("[INF] FileScheduler.compressSyncResource() : [OK] Building compression of rescoures' files! ......")
					pSelf.compressSyncResource()
				}
			}
		}

	}
}

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

func parseTimeStr(sTimeString string) (int, int, int, int, int, int, bool) {
	lstDateTime := strings.Split(sTimeString, " ")
	lstDate := strings.Split(lstDateTime[0], "-")
	lstTime := strings.Split(lstDateTime[1], ":")

	nYY, err := strconv.Atoi(lstDate[0])
	if nil != err {
		log.Println("[WARN] fscheduler.go.parseTimeStr() : cannot parse Year :", lstDate[0], err.Error())
		return 0, 0, 0, 0, 0, 0, false
	}

	nMM, err := strconv.Atoi(lstDate[1])
	if nil != err {
		log.Println("[WARN] fscheduler.go.parseTimeStr() : cannot parse Month :", lstDate[1], err.Error())
		return 0, 0, 0, 0, 0, 0, false
	}

	nDD, err := strconv.Atoi(lstDate[2])
	if nil != err {
		log.Println("[WARN] fscheduler.go.parseTimeStr() : cannot parse Day :", lstDate[0], err.Error())
		return 0, 0, 0, 0, 0, 0, false
	}

	nHH, err := strconv.Atoi(lstTime[0])
	if nil != err {
		log.Println("[WARN] fscheduler.go.parseTimeStr() : cannot parse Hour :", lstTime[0], err.Error())
		return 0, 0, 0, 0, 0, 0, false
	}

	nmm, err := strconv.Atoi(lstTime[1])
	if nil != err {
		log.Println("[WARN] fscheduler.go.parseTimeStr() : cannot parse Minute :", lstTime[1], err.Error())
		return 0, 0, 0, 0, 0, 0, false
	}

	nSS, err := strconv.Atoi(lstTime[2][:2])
	if nil != err {
		log.Println("[WARN] fscheduler.go.parseTimeStr() : cannot parse Second :", lstTime[2], err.Error())
		return 0, 0, 0, 0, 0, 0, false
	}

	return nYY, nMM, nDD, nHH, nmm, nSS, true
}

///////////////////////////////////// [InnerMethod]
func (pSelf *FileScheduler) compressSyncResource() bool {
	objNowTime := time.Now()
	objBuildTime := time.Date(objNowTime.Year(), objNowTime.Month(), objNowTime.Day(), pSelf.BuildTime/10000, pSelf.BuildTime/100%100, pSelf.BuildTime%100, 0, time.Local)

	/////////////////////////////// Judge Whether 2 Compress Quotation Files
	objStatusLoader, err := os.Open("./status.dat")
	defer objStatusLoader.Close()
	if nil == err {
		bytesData := make([]byte, 20)
		nLen, _ := objStatusLoader.Read(bytesData)
		log.Printf("[INF] FileScheduler.compressSyncResource() : [OK] Load %d bytes from ./status.dat ---> %s", nLen, string(bytesData))
		nYY, nMM, nDD, _, _, _, bIsOk := parseTimeStr(string(bytesData))
		if true == bIsOk {
			log.Println("[INF] FileScheduler.compressSyncResource() : date in ./status.dat ---> ", nYY, nMM, nDD)
			if objNowTime.Year() == nYY && int(objNowTime.Month()) == nMM && int(objNowTime.Day()) == nDD {
				log.Println("[INF] FileScheduler.compressSyncResource() : [OK] Skip compression of rescoures' files! ......")
				return true
			}
		}
	}

	/////////////////////////////// Judge Whether 2 Compress A New Resoures(.tar.gz) Or Not
	if pSelf.LastUpdateTime.Year() != objBuildTime.Year() || pSelf.LastUpdateTime.Month() != objBuildTime.Month() || pSelf.LastUpdateTime.Day() != objBuildTime.Day() {
		if objNowTime.After(objBuildTime) == true {
			var objNewResList ResourceList
			var objCompressor Compressor = Compressor{TargetFolder: pSelf.SyncFolder}
			log.Printf("[INF] FileScheduler.compressSyncResource() : (BuildTime=%s) Building Sync Resources ......", time.Now().Format("2006-01-02 15:04:05"))

			/////////////////////// iterate data source configuration && compress quotation files
			for sResName, objDataSrcCfg := range pSelf.DataSourceConfig {
				lstRes, bIsOk := objCompressor.XCompress(sResName, &objDataSrcCfg, pSelf.GetRangeOP(sResName))
				if true == bIsOk {
					/////////////// record resource path && MD5 which has been compressed
					objNewResList.Download = append(objNewResList.Download, lstRes...)
					log.Println("[INF] FileScheduler.compressSyncResource() : [OK] TarFile : ", objDataSrcCfg.Folder)
				} else {
					log.Println("[WARN] FileScheduler.compressSyncResource() : [FAILURE] TarFile : ", objDataSrcCfg.Folder)
					return false
				}
			}

			pSelf.RefSyncSvr.SetResList(&objNewResList)
			pSelf.LastUpdateTime = time.Now() // update time
			sBuildedTime := pSelf.LastUpdateTime.Format("2006-01-02 15:04:05")
			log.Println("[INF] FileScheduler.compressSyncResource() : [OK] Sync Resources Builded! ......", sBuildedTime)

			//////////////////////// save status 2 ./status.dat
			objStatusSaver, err := os.Create("./status.dat")
			defer objStatusSaver.Close()
			if nil != err {
				log.Println("[ERROR] FileScheduler.compressSyncResource() : [FAILURE] cannot save ./status.dat 2 disk :", err.Error())
			} else {
				nLen, _ := objStatusSaver.Write([]byte(sBuildedTime))
				log.Printf("[INF] FileScheduler.compressSyncResource() : [OK] Write %d bytes 2 ./status.dat <--- %s", nLen, sBuildedTime)
			}
		}
	}

	return true
}
