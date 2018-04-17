/**
 * @brief		File's Comparison Tools
 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
	"log"
)

var ()

// Package Initialization
func init() {
}

///////////////////////////////////// HTTP Client Engine Stucture/Class
type FComparison struct {
}

///////////////////////////////////// [OutterMethod]
//  Active HTTP Client
func (pSelf *FComparison) DoTasks() {
	log.Println("[INF] FComparison.DoTasks() : Executing Tasks ...... ")

}

///////////////////////////////////// [InnerMethod]
// [method] download resource
func (pSelf *FComparison) fetchResource(sUri, sMD5, sDateTime string) {
	log.Println("[INF] FComparison.fetchResource() : [Downloading] -->", sUri, sMD5, sDateTime)

	log.Println("[INF] FComparison.fetchResource() : [Complete]")
}
