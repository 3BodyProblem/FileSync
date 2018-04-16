/**
 * @brief		Engine Of Client
 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/cookiejar"
)

var (
	globalCurrentCookies   []*http.Cookie // Current Cookie
	globalCurrentCookieJar *cookiejar.Jar // Current CookieJar
)

// Package Initialization
func init() {
	globalCurrentCookies = nil
	globalCurrentCookieJar, _ = cookiejar.New(nil)
}

///////////////////////////////////// HTTP Client Engine Stucture/Class
type FileSyncClient struct {
	ServerHost string // Server IP + Port
	Account    string // Server Login Username
	Password   string // Server Login Password
}

///////////////////////////////////// [OutterMethod]
//  Active HTTP Client
func (pSelf *FileSyncClient) DoTasks() {
	log.Println("[INF] FileSyncClient.DoTasks() : Executing Tasks ...... ")
	pSelf.login2Server()

}

///////////////////////////////////// [InnerMethod]
// [Event] login 2 server
func (pSelf *FileSyncClient) login2Server() bool {
	// generate Login Url string
	var sUrl string = fmt.Sprintf("http://%s/login?account=%s&password=%s", pSelf.ServerHost, pSelf.Account, pSelf.Password)
	log.Println("[INF] FileSyncClient.login2Server() :  [GET] ", sUrl)

	// declare http request variable
	httpClient := http.Client{
		CheckRedirect: nil,
		Jar:           globalCurrentCookieJar,
	}
	httpReq, err := http.NewRequest("GET", sUrl, nil)
	httpRes, err := httpClient.Do(httpReq)

	if err != nil {
		log.Println("[ERR] FileSyncClient.login2Server() :  Error In Response : ", sUrl, err.Error())
		return false
	}

	// parse && read response string
	defer httpRes.Body.Close()
	body, err := ioutil.ReadAll(httpRes.Body)
	if err != nil {
		fmt.Printf("get response for url=%s got error=%s\n", sUrl, err.Error())
	}

	// set the current cookies
	globalCurrentCookies = globalCurrentCookieJar.Cookies(httpReq.URL)

	var xmlRes struct {
		XMLName xml.Name `xml:"login"`
		Result  struct {
			XMLName xml.Name `xml:"result"`
			Status  string   `xml:"status,attr"`
			Desc    string   `xml:"desc,attr"`
		}
	} // Build Response Xml Structure

	// Marshal Obj 2 Xml String && Write 2 HTTP Response Object
	if err := xml.Unmarshal(body, &xmlRes); err != nil {
		log.Println("[ERR] FileSyncClient.login2Server() : ", err.Error())
		log.Println("[ERR] FileSyncClient.login2Server() : ", body)
	} else {
		if xmlRes.Result.Status == "success" {
			return true
		}

		log.Println("[WARN] FileSyncClient.login2Server() : ", string(body))
	}

	/*
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
	*/

	return false
}
