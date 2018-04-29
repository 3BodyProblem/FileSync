/**
 * @brief		Engine Of Server
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"./github.com/astaxie/beego/session"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
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

type ResDownload struct {
	XMLName xml.Name `xml:"download"`
	URI     string   `xml:"uri,attr"`
	MD5     string   `xml:"md5,attr"`
	UPDATE  string   `xml:"update,attr"`
}

type ResourceList struct {
	XMLName  xml.Name `xml:"resource"`
	Download []ResDownload
} // Build Response Xml Structure

///////////////////////////////////// HTTP Server Engine Stucture/Class
type FileSyncServer struct {
	ServerHost      string       // Server IP + Port
	Account         string       // Server Login Username
	Password        string       // Server Login Password
	SyncFolder      string       // Sync File Folder
	objResourceList ResourceList // Resources Table
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

func (pSelf *FileSyncServer) SetResList(refResList *ResourceList) {
	pSelf.objResourceList = *refResList
}

///////////////////////////////////// [InnerMethod]
// Authenticate User's Session
func (pSelf *FileSyncServer) authenticateSession(resp http.ResponseWriter, req *http.Request) bool {
	req.ParseForm()
	objSession, _ := globalSessions.SessionStart(resp, req)
	defer objSession.SessionRelease(resp)
	sUNameInSS := objSession.Get("username")

	if sUNameInSS == nil {
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

	return true
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
	var xmlRes struct {
		XMLName xml.Name `xml:"download"`
		Result  struct {
			XMLName xml.Name `xml:"result"`
			Status  string   `xml:"status,attr"`
			Desc    string   `xml:"desc,attr"`
		}
	} // Build Response Xml Structure

	if pSelf.authenticateSession(resp, req) == false {
		return
	}

	// Initialize Arguments
	req.ParseForm()

	// Download Zip File
	if len(req.Form["uri"]) > 0 {
		sZipName = req.Form["uri"][0]
		resp.Header().Set("Content-Type", "application/zip")
		resp.Header().Set("Content-Encoding", "zip")
		resp.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", sZipName))
		dataRes, err := ioutil.ReadFile(sZipName)
		if err == nil {
			resp.Write(dataRes)
		} else {
			xmlRes.Result.Status = "failure"
			xmlRes.Result.Desc = "[WARNING] Oops! failed 2 load data file," + sZipName
			// Marshal Obj 2 Xml String && Write 2 HTTP Response Object
			if sResponse, err := xml.Marshal(&xmlRes); err != nil {
				fmt.Fprintf(resp, "%s", err.Error())
			} else {
				fmt.Fprintf(resp, "%s%s", xml.Header, string(sResponse))
			}
		}
	} else {
		xmlRes.Result.Status = "failure"
		xmlRes.Result.Desc = "[WARNING] Oops! miss argument, GET: uri=''"
		log.Println("[INF] [Download File] ---> [FAILURE], miss argument, GET: uri='nil'")

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

	// Marshal Obj 2 Xml String && Write 2 HTTP Response Object
	if sResponse, err := xml.Marshal(&pSelf.objResourceList); err != nil {
		fmt.Fprintf(resp, "%s")
	} else {
		fmt.Fprintf(resp, "%s%s", xml.Header, string(sResponse))
	}
}
