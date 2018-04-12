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
)

func init() {
}

///////////////////////////////////// File Scheduler Stucture/Class
type FileScheduler struct {
	XmlCfgPath string // Sync File Folder
}

///////////////////////////////////// [OutterMethod]
//  Active File Scheduler
func (pSelf *FileScheduler) RunServer() bool {
	log.Println("[INF] FileScheduler.RunServer() : configuration file path: ", pSelf.XmlCfgPath)

	// Definition Of Profile's Structure
	var objCfg struct {
		XMLName xml.Name `xml:"cfg"`
		Setting struct {
			XMLName xml.Name `xml:setting`
			name    string   `xml:"name,attr"`
			value   string   `xml:"value,attr"`
		}
	}

	// Analyze configuration(.xml) 4 Engine
	sXmlContent, err := ioutil.ReadFile(pSelf.XmlCfgPath)
	if err != nil {
		log.Println("[WARN] FileScheduler.RunServer() : cannot locate configuration file, path: ", pSelf.XmlCfgPath)
		return false
	}

	err = xml.Unmarshal(sXmlContent, &objCfg)
	if err != nil {
		log.Println("[WARN] FileScheduler.RunServer() : cannot parse xml configuration file, error: ", err.Error())
		return false
	}

	// Extract Settings
	/*	for i, line := range v.Setting {
			if strings.EqualFold(line.StringName, "ApplicationName") {
				fmt.Println("change innerText")
				result.ResourceString[i].InnerText = "这是新的ApplicationName"
			}
		}
	*/

	return true
}
