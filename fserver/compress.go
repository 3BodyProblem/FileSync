/**
 * @brief		File's Compress Tools
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"archive/zip"
	//"crypto/md5"
	"io/ioutil"
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
type Compress struct {
	TargetFolder string // Root Folder
}

///////////////////////////////////// [OutterMethod]
// [method] Zip
func (pSelf *Compress) Zip(sResName string, objDataSrc *DataSourceConfig) bool {
	var sDataType string = strings.ToLower(sResName[strings.Index(sResName, "."):])              // data type (d1/m1/m5)
	var sDestFolder string = filepath.Join(pSelf.TargetFolder, strings.ToUpper(objDataSrc.MkID)) // target folder of data(.zip)
	log.Printf("[INF] Compress.Zip() : [Compressing] ExchangeCode:%s, DataType:%s, Folder:%s", objDataSrc.MkID, sDataType, objDataSrc.Folder)

	_, sFileName := path.Split(sDestFolder)
	switch {
	case (objDataSrc.MkID == "sse" && sDataType == ".d1") || (objDataSrc.MkID == "szse" && sDataType == ".d1"):
		pSelf.zipFolder(filepath.Join(sDestFolder, sFileName+".zip"), objDataSrc.Folder)
	case (objDataSrc.MkID == "sse" && sDataType == ".m1") || (objDataSrc.MkID == "szse" && sDataType == ".m1"):
	default:
		log.Printf("[ERR] Compress.Zip() : [Compressing] invalid exchange code(%s) or data type(%s)", objDataSrc.MkID, sDataType)
		return false
	}
	/*
		sZipSrcPath := ""
		// open zip file
		sLocalFolder := path.Dir(filepath.Join(pSelf.TargetFolder, objDataSrc.Folder))
		objZipReader, err := zip.OpenReader(sZipSrcPath)
		if err != nil {
			log.Println("[ERR] Compress.Zip() : [Compressing] cannot open zip file :", sZipSrcPath, err.Error())
			return false
		}

		defer objZipReader.Close()
		for _, objFile := range objZipReader.File {
			objReadCloser, err := objFile.Open()
			if err != nil {
				log.Println("[ERR] Compress.Zip() : [Compressing] cannot open file in zip package, file name =", objFile.Name)
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
				log.Println("[ERR] Compress.Zip() : [Compressing] cannot build target folder 4 zip file, file name =", sTargetFile)
				return false
			}

			objTargetFile, err := os.Create(sTargetFile)
			if err != nil {
				log.Println("[ERR] Compress.Zip() : [Compressing] cannot create zip file in target folder, file name =", sTargetFile)
				return false
			}
			defer objTargetFile.Close()
			_, err = io.Copy(objTargetFile, objReadCloser)
			if err != nil {
				log.Println("[ERR] v.Zip() : [Compressing] cannot write date 2 zip file in target folder, file name =", sTargetFile)
				return false
			}

			objTargetFile.Close()
			objReadCloser.Close()
		}
	*/
	return true
}

///////////////////////////////////// [InnerMethod]
// [method] Zip
func (pSelf *Compress) zipFolder(sDestFile, sSrcFolder string) bool {
	err := os.MkdirAll(path.Dir(sDestFile), 0755)
	if err != nil {
		log.Println("[ERR] Compress.zipFolder() : cannot build target folder 4 zip file :", path.Dir(sDestFile))
		return false
	}

	objZipFile, err := os.Create(sDestFile)
	objZipWriter := zip.NewWriter(objZipFile)
	defer objZipFile.Close()
	defer objZipWriter.Close()

	log.Printf("[INF] Compress.zipFolder() : zipping (%s) --> (%s)", sSrcFolder, sDestFile)
	if err != nil {
		log.Println("[ERR] Compress.zipFolder() : failed 2 create zip file :", sDestFile, err.Error())
		return false
	}

	err = filepath.Walk(sSrcFolder, func(sPath string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}

		if f.IsDir() {
			return nil
		}

		// get absolute path of URI in local machine
		objFile, err := os.Open(sPath)
		if err != nil {
			log.Println("[WARN] Compress.zipFolder() : local file is not exist :", sPath)
			return nil
		}

		defer objFile.Close()
		_, sFileName := path.Split(sPath)
		objInserter, err := objZipWriter.Create(sFileName)
		if err != nil {
			log.Println("[WARN] Compress.zipFolder() : failed 2 add file :", sPath)
			return nil
		}

		bytesData, err := ioutil.ReadFile(sPath)
		if err != nil {
			log.Println("[WARN] Compress.zipFolder() : failed 2 read file=", sPath)
			return nil
		}

		_, err = objInserter.Write(bytesData)
		if err != nil {
			log.Println("[WARN] Compress.zipFolder() : failed 2 write file into zip package :", sPath)
			return nil
		}

		return nil
	})

	if err != nil {
		log.Println("[ERR] Compress.zipFolder() : failed 2 walk src folder :", sSrcFolder)
		return false
	}

	return true
}
