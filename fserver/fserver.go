/**
 * @brief		Engine Of Server
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"./github.com/astaxie/beego/session"
	"archive/zip"
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

var (
	globalSessions *session.Manager = nil // Global Session Manager
)

// Package Initialization
func init() {
	// @Note: Initialize Session Manager 4 Web Server && Active Its' Garbage Collection Routine
	globalSessions, _ = session.NewManager("memory", &session.ManagerConfig{CookieName: "FileSyncSSID", EnableSetCookie: true, Gclifetime: 3600 * 10, Maxlifetime: 3600 * 10, Secure: false, CookieLifeTime: 3600 * 10, ProviderConfig: ""})
	go globalSessions.GC()
}

///////////////////////////////////// HTTP Server Engine Stucture/Class
type FileSyncServer struct {
	ServerHost string // Server IP + Port
	Account    string // Server Login Username
	Password   string // Server Login Password
	SyncFolder string // Sync File Folder
}

///////////////////////////////////// [OutterMethod]
//  Active HTTP Server
func (pSelf *FileSyncServer) RunServer() {
	// Create a http server && Register Http Event
	http.HandleFunc("/", pSelf.handleDefault)
	http.HandleFunc("/login", pSelf.handleLogin)
	http.HandleFunc("/get", pSelf.handleDownload)
	http.HandleFunc("/list", pSelf.handleList)

	// Active the http server
	log.Println("[INF] FileSyncServer.RunServer() : Sync Folder :", pSelf.SyncFolder)
	log.Println("[INF] FileSyncServer.RunServer() : Server Is Available [", pSelf.ServerHost, "] .........")
	http.ListenAndServe(pSelf.ServerHost, nil)
	log.Println("[INF] FileSyncServer.RunServer() : Server Has Halted.........")
}

///////////////////////////////////// [InnerMethod]
// Authenticate User's Session
func (pSelf *FileSyncServer) authenticateSession(resp http.ResponseWriter, req *http.Request) bool {
	req.ParseForm()
	objSession, _ := globalSessions.SessionStart(resp, req)
	defer objSession.SessionRelease(resp)
	sUNameInSS := objSession.Get("username")

	if sUNameInSS != nil {
		log.Println("[INF] [AuthenticateUser] ---> [OK]: ", sUNameInSS)
		return true
	} else {
		var xmlRes struct {
			XMLName xml.Name `xml:"authenticate"`
			Result  struct {
				XMLName xml.Name `xml:"result"`
				Status  string   `xml:"status,attr"`
				Desc    string   `xml:"desc,attr"`
			}
		} // Build Response Xml Structure

		xmlRes.Result.Status = "failure"
		xmlRes.Result.Desc = "[WARNING] Oops! user session has expired."
		log.Println("[INF] [AuthenticateUser] ---> [FAILURE]")

		// Marshal Obj 2 Xml String && Write 2 HTTP Response Object
		if sResponse, err := xml.Marshal(&xmlRes); err != nil {
			fmt.Fprintf(resp, "%s", err.Error())
		} else {
			fmt.Fprintf(resp, "%s%s", xml.Header, string(sResponse))
		}

		return false
	}
}

// [Event] default
func (pSelf *FileSyncServer) handleDefault(resp http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(resp, "Server Of File Sync Program.\n\nUsage Of Action:\n\nhttp://127.0.0.1/login?account=xx&password=xxx\n\nhttp://127.0.0.1/get?uri=xxx.zip\n\nhttp://127.0.0.1/list\n\n")
}

// [Event] login
func (pSelf *FileSyncServer) handleLogin(resp http.ResponseWriter, req *http.Request) {
	var sAccount, sPswd string // LoginName && LoginPassword
	var xmlRes struct {
		XMLName xml.Name `xml:"login"`
		Result  struct {
			XMLName xml.Name `xml:"result"`
			Status  string   `xml:"status,attr"`
			Desc    string   `xml:"desc,attr"`
		}
	} // Build Response Xml Structure

	// Initialize Arguments
	req.ParseForm()
	objSession, _ := globalSessions.SessionStart(resp, req)
	defer objSession.SessionRelease(resp)
	sUNameInSS := objSession.Get("username")

	// Check Login Status
	if sUNameInSS != nil {
		xmlRes.Result.Status = "success"
		xmlRes.Result.Desc = "[INFO] welcome again"
		log.Println("[INF] HttpAction[Relogin], [OK]: ", sUNameInSS)
	} else {
		// Fetch Aruguments ( LoginName && LoginPassword )
		xmlRes.Result.Status = "failure"
		xmlRes.Result.Desc = "[WARNING] Oops! account or password r incorrect."
		if len(req.Form["account"]) > 0 {
			sAccount = req.Form["account"][0]
		}

		if len(req.Form["password"]) > 0 {
			sPswd = req.Form["password"][0]
		}

		// Check LoginName && LoginPassword
		if pSelf.Account == sAccount && pSelf.Password == sPswd {
			objSession.Set("username", sAccount)
			xmlRes.Result.Status = "success"
			xmlRes.Result.Desc = "[INFO] Good! account and password r all correct."
			log.Println("[INF] HttpAction[Login], [OK]: ", sAccount)
		} else {
			log.Println("[INF] HttpAction[Login], [FAILED]: ", sAccount)
		}
	}

	// Marshal Obj 2 Xml String && Write 2 HTTP Response Object
	if sResponse, err := xml.Marshal(&xmlRes); err != nil {
		fmt.Fprintf(resp, "%s", err.Error())
	} else {
		fmt.Fprintf(resp, "%s%s", xml.Header, string(sResponse))
	}
}

// [Event] Download
func (pSelf *FileSyncServer) handleDownload(resp http.ResponseWriter, req *http.Request) {
	var sZipName string = ""

	if pSelf.authenticateSession(resp, req) == false {
		return
	}

	// Initialize Arguments
	req.ParseForm()

	// Download Zip File
	if len(req.Form["uri"]) > 0 {
		sZipName = req.Form["uri"][0]
		resp.Header().Set("Content-Type", "application/zip")
		resp.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", sZipName))

		objZipWriter := zip.NewWriter(resp)
		defer objZipWriter.Close()

		/*
		   for i := 0; i < 5; i++ {
		       f, err := zipW.Create(strconv.Itoa(i) + ".txt")
		       if err != nil {
		           return err
		       }
		       _, err = f.Write([]byte(fmt.Sprintf("Hello file %d", i)))
		       if err != nil {
		           return err
		       }
		   }
		*/
	} else {
		var xmlRes struct {
			XMLName xml.Name `xml:"download"`
			Result  struct {
				XMLName xml.Name `xml:"result"`
				Status  string   `xml:"status,attr"`
				Desc    string   `xml:"desc,attr"`
			}
		} // Build Response Xml Structure

		xmlRes.Result.Status = "failure"
		xmlRes.Result.Desc = "[WARNING] Oops! miss argument, GET: uri=''"
		log.Println("[INF] [Download File] ---> [FAILURE], miss argument, GET: uri=''")

		// Marshal Obj 2 Xml String && Write 2 HTTP Response Object
		if sResponse, err := xml.Marshal(&xmlRes); err != nil {
			fmt.Fprintf(resp, "%s", err.Error())
		} else {
			fmt.Fprintf(resp, "%s%s", xml.Header, string(sResponse))
		}
	}

}

// [Event] List Resouces
func (pSelf *FileSyncServer) handleList(resp http.ResponseWriter, req *http.Request) {
	if pSelf.authenticateSession(resp, req) == false {
		return
	}

	type ResDownload struct {
		XMLName xml.Name `xml:"download"`
		URI     string   `xml:"uri,attr"`
		MD5     string   `xml:"md5,attr"`
		UPDATE  string   `xml:"update,attr"`
	}

	var objResourceList struct {
		XMLName  xml.Name `xml:"resource"`
		Download []ResDownload
	} // Build Response Xml Structure

	err := filepath.Walk(pSelf.SyncFolder, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}

		if f.IsDir() {
			return nil
		}

		objResourceList.Download = append(objResourceList.Download, ResDownload{URI: path, UPDATE: time.Now().Format("2006-01-02 15:04:05")})
		return nil
	})

	if err != nil {
		fmt.Fprintf(resp, "%s%s", xml.Header, err.Error())
	}

	// Marshal Obj 2 Xml String && Write 2 HTTP Response Object
	if sResponse, err := xml.Marshal(&objResourceList); err != nil {
		fmt.Fprintf(resp, "%s")
	} else {
		fmt.Fprintf(resp, "%s%s", xml.Header, string(sResponse))
	}
}
