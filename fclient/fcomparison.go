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
	"path"
	"path/filepath"
	"strings"
)

var (
	CacheFolder string = "./FileCache" // cache folder of the program
)

type FileDescType int // 任务类型描述值

const (
	FD_IsNotExist FileDescType = iota // 0: 文件不存在
	FD_IsExist                        // 1: 文件存在
)

// Package Initialization
func init() {
}

///////////////////////////////////// HTTP Client Engine Stucture/Class
type FComparison struct {
	URI          string // Resource URI in server
	MD5          string // MD5 of Res
	DateTime     string // UpdateTime Of Res
	TargetFolder string // Target folder 4 extracted data
}

///////////////////////////////////// [OutterMethod]
// [method] Compare Resource Files Between Server & Client
func (pSelf *FComparison) Compare() (bool, FileDescType) {
	var nFileDescType FileDescType = FD_IsNotExist
	// get absolute path of current working folder && build it
	sLocalFolder, err := filepath.Abs((filepath.Dir("./")))
	if err != nil {
		log.Println("[WARN] FComparison.Compare() : failed 2 fetch absolute path of program")
		return false, nFileDescType
	}

	sLocalFolder = filepath.Join(sLocalFolder, CacheFolder)
	//log.Printf("[INF] FComparison.Compare() : [Comparing] (%s)  VS  (%s) ", pSelf.URI, filepath.Join(sLocalFolder, pSelf.URI))
	// get absolute path of URI in local machine
	sLocalFile := filepath.Join(sLocalFolder, pSelf.URI)
	objFile, err := os.Open(sLocalFile)
	if err != nil {
		return false, nFileDescType // log.Println("[INF] FComparison.Compare() : local resource is not exist :", sLocalFile)
	}

	nFileDescType = FD_IsExist
	// parepare 2 generate md5
	defer objFile.Close()
	objMD5Hash := md5.New()
	if _, err := io.Copy(objMD5Hash, objFile); err != nil {
		log.Printf("[WARN] FComparison.Compare() : failed 2 generate MD5 : %s : %s", sLocalFile)
		return false, nFileDescType
	}

	// generate MD5 string
	var byteMD5 []byte
	var sMD5Str string = fmt.Sprintf("%x", objMD5Hash.Sum(byteMD5))

	// result
	if strings.ToLower(pSelf.MD5) != strings.ToLower(sMD5Str) {
		log.Printf("[INF] FComparison.Compare() : found a discrepancy of md5 between server(url:%s) && client(md5:%s)", strings.ToLower(pSelf.URI), strings.ToLower(sMD5Str))
		return false, nFileDescType
	}

	return true, nFileDescType
}

func (pSelf *FComparison) ClearCacheFolder() bool {
	// get absolute path of current working folder && build it
	sLocalFolder, err := filepath.Abs((filepath.Dir("./")))
	if err != nil {
		log.Println("[WARN] FComparison.ClearCacheFolder() : failed 2 fetch absolute path of program")
		return false
	}

	sLocalFolder = filepath.Join(sLocalFolder, CacheFolder)
	// get absolute path of URI in local machine
	sLocalFile := filepath.Join(sLocalFolder, pSelf.URI)
	sLocalFolder = path.Dir(sLocalFile)
	log.Printf("[INF] FComparison.ClearCacheFolder() : Clearing... %s for %s", sLocalFolder, pSelf.URI)
	err = os.RemoveAll(sLocalFolder)
	if err != nil {
		log.Printf("[WARN] FComparison.ClearCacheFolder() : An error occur while clearing %s for %s", sLocalFolder, pSelf.URI)
		return false
	} else {
		log.Printf("[INF] FComparison.ClearCacheFolder() : Cleared... %s for %s", sLocalFolder, pSelf.URI)
	}

	return true
}

func (pSelf *FComparison) ClearDataFolder() bool {
	// get absolute path of current working folder && build it
	sLocalFolder, err := filepath.Abs((filepath.Dir("./")))
	if err != nil {
		log.Println("[WARN] FComparison.ClearDataFolder() : failed 2 fetch absolute path of program")
		return false
	}

	sLocalFolder = filepath.Join(sLocalFolder, pSelf.TargetFolder)
	// get absolute path of URI in local machine
	sLocalFile := filepath.Join(sLocalFolder, pSelf.URI)
	sLocalFolder = path.Dir(sLocalFile)
	log.Printf("[INF] FComparison.ClearDataFolder() : Clearing... %s for %s", sLocalFolder, pSelf.URI)
	err = os.RemoveAll(sLocalFolder)
	if err != nil {
		log.Printf("[WARN] FComparison.ClearDataFolder() : An error occur while clearing %s for %s", sLocalFolder, pSelf.URI)
		return false
	} else {
		log.Printf("[INF] FComparison.ClearDataFolder() : Cleared... %s for %s", sLocalFolder, pSelf.URI)
	}

	return true
}
