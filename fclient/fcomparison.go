/**
 * @brief		File's Comparison Tools
 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"
)

var ()

// Package Initialization
func init() {
}

///////////////////////////////////// HTTP Client Engine Stucture/Class
type FComparison struct {
	ServerHost string // Server IP + Port
	Account    string // Server Login Username
	Password   string // Server Login Password
	TTL        int    // Time To Live
}

///////////////////////////////////// [OutterMethod]
//  Active HTTP Client
func (pSelf *FileSyncClient) DoTasks() {
	log.Println("[INF] FileSyncClient.DoTasks() : Executing Tasks ...... ")

}

///////////////////////////////////// [InnerMethod]
// [method] download resource
func (pSelf *FComparison) fetchResource(sUri, sMD5, sDateTime string) {
	log.Println("[INF] FileSyncClient.fetchResource() : [Downloading] -->", sUri, sMD5, sDateTime)

	log.Println("[INF] FileSyncClient.fetchResource() : [Complete]")
	pSelf.objChannel <- DownloadStatus{URI: sUri, Status: ST_Completed} // Mission Complete!
}
