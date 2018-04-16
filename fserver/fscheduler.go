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
	"strings"
)

func init() {
}

type DataSourceConfig struct {
	MkID   string // market id ( SSE:shanghai SZSE:shenzheng )
	Folder string // data file folder
}

///////////////////////////////////// File Scheduler Stucture/Class
type FileScheduler struct {
	XmlCfgPath       string                      // Xml Configuration File Path
	SyncFolder       string                      // Sync File Folder
	DataSourceConfig map[string]DataSourceConfig // Data Source Config Of Markets
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

	// Analyze configuration(.xml) 4 Engine
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

	// Extract Settings
	log.Println("[INF] FileScheduler.Active() : configuration file version: ", objCfg.Version)
	pSelf.DataSourceConfig = make(map[string]DataSourceConfig)
	for _, objSetting := range objCfg.Setting {
		switch strings.ToLower(objSetting.Name) {
		case "syncfolder":
			pSelf.SyncFolder = objSetting.Value
			log.Println("[INF] FileScheduler.Active() : SyncFolder: ", pSelf.SyncFolder)
		default:
			sSetting := strings.ToLower(objSetting.Name)
			if len(strings.Split(objSetting.Name, ".")) <= 1 {
				log.Println("[WARN] FileScheduler.Active() : [Xml.Setting] Ignore -> ", objSetting.Name)
				continue
			}

			pSelf.DataSourceConfig[sSetting] = DataSourceConfig{MkID: strings.Split(objSetting.Name, ".")[0], Folder: objSetting.Value}
			log.Println("[INF] FileScheduler.Active() : [Xml.Setting] ", sSetting, pSelf.DataSourceConfig[sSetting].MkID, pSelf.DataSourceConfig[sSetting].Folder)
		}
	}

	return pSelf.DataSourceConfig != nil
}
