/**
 * @brief		File's Comparison Tools
 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
	"archive/zip"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// Package Initialization
func init() {
}

///////////////////////////////////// HTTP Client Engine Stucture/Class
type Uncompress struct {
	TargetFolder string // Root Folder
}

///////////////////////////////////// [OutterMethod]
// [method] Unzip
func (pSelf *Uncompress) Unzip(sZipSrcPath, sSubPath string) bool {
	log.Printf("[INF] Uncompress.Unzip() : [Uncompressing] (%s) --> (%s) ", sZipSrcPath, pSelf.TargetFolder)

	// open zip file
	sLocalFolder := path.Dir(filepath.Join(pSelf.TargetFolder, sSubPath))
	objZipReader, err := zip.OpenReader(sZipSrcPath)
	if err != nil {
		log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot open zip file :", sZipSrcPath, err.Error())
		return false
	}

	defer objZipReader.Close()
	for _, objFile := range objZipReader.File {
		objReadCloser, err := objFile.Open()
		if err != nil {
			log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot open file in zip package, file name =", objFile.Name)
			return false
		}

		defer objReadCloser.Close()
		sTargetFile := filepath.Join(sLocalFolder, objFile.Name)
		_, sSplitFileName := path.Split(sTargetFile)
		if strings.Contains(sSplitFileName, ".") == false {
			continue
		}
		sTargetFolder := path.Dir(sTargetFile)
		err = os.MkdirAll(sTargetFolder, 0755)
		if err != nil {
			log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot build target folder 4 zip file, file name =", sTargetFile)
			return false
		}

		log.Println("[INF] Uncompress.Unzip() : [Uncompressing] creating zip file in target folder, file name =", sTargetFile)
		objTargetFile, err := os.Create(sTargetFile)
		if err != nil {
			log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot create zip file in target folder, file name =", sTargetFile)
			return false
		}
		defer objTargetFile.Close()
		_, err = io.Copy(objTargetFile, objReadCloser)
		if err != nil {
			log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot write date 2 zip file in target folder, file name =", sTargetFile)
			return false
		}

		objTargetFile.Close()
		objReadCloser.Close()
		log.Printf("[INF] Uncompress.Unzip() : [Uncompressed] (%s) --> (%s) ", sZipSrcPath, sTargetFile)
	}

	return true
}

///////////////////////////////////// [InnerMethod]
