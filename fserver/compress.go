/**
 * @brief		File's Compress Tools
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"archive/zip"
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
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

	switch {
	case (objDataSrc.MkID == "sse" && sDataType == ".d1") || (objDataSrc.MkID == "szse" && sDataType == ".d1"):
		pSelf.zipFolder(filepath.Join(sDestFolder, "DAY.zip"), objDataSrc.Folder)
	case (objDataSrc.MkID == "sse" && sDataType == ".m1") || (objDataSrc.MkID == "szse" && sDataType == ".m1"):
		pSelf.zipM1Folder(filepath.Join(sDestFolder, "MIN.zip"), objDataSrc.Folder)
	case (objDataSrc.MkID == "sse" && sDataType == ".m5") || (objDataSrc.MkID == "szse" && sDataType == ".m5"):
		pSelf.zipM5Folder(filepath.Join(sDestFolder, "MIN5.zip"), objDataSrc.Folder)
	default:
		log.Printf("[ERR] Compress.Zip() : [Compressing] invalid exchange code(%s) or data type(%s)", objDataSrc.MkID, sDataType)
		return false
	}

	return true
}

///////////////////////////////////// [InnerMethod]
// [method] Zip M5
func (pSelf *Compress) zipM5Folder(sDestFile, sSrcFolder string) bool {
	err := os.MkdirAll(path.Dir(sDestFile), 0755)
	if err != nil {
		log.Println("[ERR] Compress.zipM5Folder() : cannot build target folder 4 zip file :", path.Dir(sDestFile))
		return false
	}

	sSubFolder := "MIN5"
	objZipFile, err := os.Create(sDestFile)
	objZipWriter := zip.NewWriter(objZipFile)
	defer objZipFile.Close()
	defer objZipWriter.Close()

	log.Printf("[INF] Compress.zipM5Folder() : zipping (%s) --> (%s)", sSrcFolder, sDestFile)
	if err != nil {
		log.Println("[ERR] Compress.zipM5Folder() : failed 2 create zip file :", sDestFile, err.Error())
		return false
	}

	nToday := time.Now().Year()
	err = filepath.Walk(sSrcFolder, func(sPath string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}

		if f.IsDir() {
			_, sSF := path.Split(sPath)
			sSubFolder = filepath.Join("MIN5", sSF)
			return nil
		}

		// get absolute path of URI in local machine
		objFile, err := os.Open(sPath)
		if err != nil {
			log.Println("[WARN] Compress.zipM5Folder() : local file is not exist :", sPath)
			return nil
		}

		defer objFile.Close()
		info, err := objFile.Stat()
		objHInfo, err := zip.FileInfoHeader(info)
		if err != nil {
			log.Println("[WARN] Compress.zipM5Folder() : failed 2 create file info head :", err.Error())
			return nil
		}

		_, sFileName := path.Split(sPath)
		objHInfo.Name = filepath.Join(sSubFolder, sFileName)
		objHeader, err := objZipWriter.CreateHeader(objHInfo)
		if err != nil {
			log.Println("[WARN] Compress.zipM5Folder() : failed 2 create filehead :", err.Error())
			return nil
		}

		var lstRecords []string // 5 minutes k-line
		bytesData, err := ioutil.ReadAll(objFile)
		if err != nil {
			log.Println("[WARN] Compress.zipM5Folder() : failed 2 read file=", sPath)
			return nil
		}
		for _, bLine := range bytes.Split(bytesData, []byte("\n")) {
			lstRecords = strings.Split(string(bLine), ",")
			sFirstFields := lstRecords[0]
			if len(sFirstFields) <= 0 {
				continue
			}
			nDate, err := strconv.Atoi(sFirstFields)
			if err != nil {
				continue
			}
			nDate = nDate / 10000
			if (nToday - nDate) > 1 {
				continue
			}

			// cal. 5 minutes k-lines

			_, err = objHeader.Write(bLine)
			if err != nil {
				log.Println("[WARN] Compress.zipM5Folder() : failed 2 write zip file=", sPath)
				return nil
			}
		}

		return nil
	})

	if err != nil {
		log.Println("[ERR] Compress.zipM5Folder() : failed 2 walk src folder :", sSrcFolder)
		return false
	}

	return true
}

// [method] Zip M1
func (pSelf *Compress) zipM1Folder(sDestFile, sSrcFolder string) bool {
	err := os.MkdirAll(path.Dir(sDestFile), 0755)
	if err != nil {
		log.Println("[ERR] Compress.zipM1Folder() : cannot build target folder 4 zip file :", path.Dir(sDestFile))
		return false
	}

	sSubFolder := "MIN"
	objZipFile, err := os.Create(sDestFile)
	objZipWriter := zip.NewWriter(objZipFile)
	defer objZipFile.Close()
	defer objZipWriter.Close()

	log.Printf("[INF] Compress.zipM1Folder() : zipping (%s) --> (%s)", sSrcFolder, sDestFile)
	if err != nil {
		log.Println("[ERR] Compress.zipM1Folder() : failed 2 create zip file :", sDestFile, err.Error())
		return false
	}

	nToday := time.Now().Year()*100 + int(time.Now().Month())
	err = filepath.Walk(sSrcFolder, func(sPath string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}

		if f.IsDir() {
			_, sSF := path.Split(sPath)
			sSubFolder = filepath.Join("MIN", sSF)
			return nil
		}

		// get absolute path of URI in local machine
		objFile, err := os.Open(sPath)
		if err != nil {
			log.Println("[WARN] Compress.zipM1Folder() : local file is not exist :", sPath)
			return nil
		}

		defer objFile.Close()
		info, err := objFile.Stat()
		objHInfo, err := zip.FileInfoHeader(info)
		if err != nil {
			log.Println("[WARN] Compress.zipM1Folder() : failed 2 create file info head :", err.Error())
			return nil
		}

		_, sFileName := path.Split(sPath)
		objHInfo.Name = filepath.Join(sSubFolder, sFileName)
		objHeader, err := objZipWriter.CreateHeader(objHInfo)
		if err != nil {
			log.Println("[WARN] Compress.zipM1Folder() : failed 2 create filehead :", err.Error())
			return nil
		}

		bytesData, err := ioutil.ReadAll(objFile)
		if err != nil {
			log.Println("[WARN] Compress.zipM1Folder() : failed 2 read file=", sPath)
			return nil
		}
		for _, bLine := range bytes.Split(bytesData, []byte("\n")) {
			sFirstFields := strings.Split(string(bLine), ",")[0]
			if len(sFirstFields) <= 0 {
				continue
			}
			nDate, err := strconv.Atoi(sFirstFields)
			if err != nil {
				continue
			}
			nDate = nDate / 100
			if (nToday - nDate) > 1 {
				continue
			}

			_, err = objHeader.Write(bLine)
			if err != nil {
				log.Println("[WARN] Compress.zipM1Folder() : failed 2 write zip file=", sPath)
				return nil
			}
		}

		return nil
	})

	if err != nil {
		log.Println("[ERR] Compress.zipM1Folder() : failed 2 walk src folder :", sSrcFolder)
		return false
	}

	return true
}

// [method] Zip Folder
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
		_, sFileName := path.Split(sDestFile)
		sSubFolder := strings.Split(sFileName, ".")[0]
		info, err := objFile.Stat()
		objHInfo, err := zip.FileInfoHeader(info)
		if err != nil {
			log.Println("[WARN] Compress.zipFolder() : failed 2 create file info head :", err.Error())
			return nil
		}

		_, sFileName = path.Split(sPath)
		objHInfo.Name = filepath.Join(sSubFolder, sFileName)
		objHeader, err := objZipWriter.CreateHeader(objHInfo)
		if err != nil {
			log.Println("[WARN] Compress.zipFolder() : failed 2 create filehead :", err.Error())
			return nil
		}

		_, err = io.Copy(objHeader, objFile)
		if err != nil {
			log.Println("[WARN] Compress.zipFolder() : failed 2 read file=", sPath)
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
