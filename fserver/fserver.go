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
	"log"
	"net/http"
)

var (
	globalSessions *session.Manager = nil // Global Session Manager
)

// Package Initialization
func init() {
	// @Note: Initialize Session Manager 4 Web Server && Active Its' Garbage Collection Routine
	globalSessions, _ = session.NewManager("memory", &session.ManagerConfig{CookieName: "FileSyncSSID", EnableSetCookie: true, Gclifetime: 3600, Maxlifetime: 3600, Secure: false, CookieLifeTime: 3600, ProviderConfig: ""})
	go globalSessions.GC()
}

///////////////////////////////////// HTTP Server Engine Stucture/Class
type FileSyncServer struct {
	ServerHost string
	Account    string
	Password   string
}

// Active HTTP Server
func (pSelf *FileSyncServer) RunServer() {
	// Create a http server [example: http.Handle("/", http.FileServer(http.Dir("./"))]
	log.Println("[INF] Server IP:Port -->", pSelf.ServerHost)
	http.HandleFunc("/", pSelf.handleDefault)
	http.HandleFunc("/login", pSelf.handleLogin)

	// Active the http server
	log.Println("[INF] Server is available......")
	http.ListenAndServe(pSelf.ServerHost, nil)
	log.Println("[INF] Server has halted......")
}

///////////////////////////////////// Net Event Handler
// event: default
func (pSelf *FileSyncServer) handleDefault(resp http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(resp, "Server Of File Sync Program.\n\nUsage Of Action:\n\n127.0.0.1/login?account=xx&password=xxx\n")
}

// event: login
func (pSelf *FileSyncServer) handleLogin(resp http.ResponseWriter, req *http.Request) {
	var sAccount, sPswd string // LoginName && LoginPassword
	var xmlRes struct {
		XMLName xml.Name `xml:"login"`
		Result  struct {
			XMLName xml.Name `xml:"result"`
			Status  string   `xml:"status,attr"`
		}
	} // Build Response Xml Structure

	// Initialize Arguments
	req.ParseForm()
	objSession, err := globalSessions.SessionStart(resp, req)
	defer objSession.SessionRelease(resp)

	// Check Login Status
	if err == nil {
		username := objSession.Get("username")
		fmt.Println("relogin", username)
		xmlRes.Result.Status = "success"
	} else {
		// Fetch Aruguments ( LoginName && LoginPassword )
		xmlRes.Result.Status = "failure"
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
		}
	}

	// Marshal Obj 2 Xml String && Write 2 HTTP Response Object
	if sResponse, err := xml.Marshal(&xmlRes); err != nil {
		fmt.Fprintf(resp, "%s%s", xml.Header, err.Error())
	} else {
		fmt.Fprintf(resp, "%s%s", xml.Header, string(sResponse))
	}
}
