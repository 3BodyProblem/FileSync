/**
 * @brief		File's Comparison Tools
 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

var (
	CacheFolder string = "./FileCache" // cache folder of the program
)

// Package Initialization
func init() {
}

///////////////////////////////////// HTTP Client Engine Stucture/Class
type FComparison struct {
	URI      string // Resource URI in server
	MD5      string // MD5 of Res
	DateTime string // UpdateTime Of Res
}

///////////////////////////////////// [OutterMethod]
// [method] Compare Resource Files Between Server & Client
func (pSelf *FComparison) Compare() bool {
	// get absolute path of current working folder && build it
	sLocalFolder, err := filepath.Abs((filepath.Dir("./")))
	if err != nil {
		log.Println("[WARN] FComparison.Compare() : failed 2 fetch absolute path of program")
		return false
	}

	sLocalFolder = filepath.Join(sLocalFolder, CacheFolder)
	log.Printf("[INF] FComparison.Compare() : [Comparing]    (%s)   VS   (%s) ", pSelf.URI, filepath.Join(sLocalFolder, pSelf.URI))

	// get absolute path of URI in local machine
	sLocalFile := filepath.Join(sLocalFolder, pSelf.URI)
	objFile, err := os.Open(sLocalFile)
	if err != nil {
		log.Println("[INF] FComparison.Compare() : local resource is not exist :", sLocalFile)
		return false
	}

	// parepare 2 generate md5
	defer objFile.Close()
	objMD5Hash := md5.New()
	if _, err := io.Copy(objMD5Hash, objFile); err != nil {
		log.Printf("[WARN] FComparison.Compare() : failed 2 generate MD5 : %s : %s", sLocalFile)
		return false
	}

	// generate MD5 string
	var byteMD5 []byte
	var sMD5Str string = fmt.Sprintf("%x", objMD5Hash.Sum(byteMD5))

	// result
	if strings.ToLower(pSelf.MD5) != strings.ToLower(sMD5Str) {
		log.Printf("[INF] FComparison.Compare() : found a discrepancy of md5 between server(md5:%s) && client(md5:%s)", strings.ToLower(pSelf.MD5), strings.ToLower(sMD5Str))
		return false
	}

	return true
}

///////////////////////////////////// [InnerMethod]
