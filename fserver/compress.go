/**
 * @brief		File's Compress Tools
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var pathSep string = "\\"

// Package Initialization
func init() {
	if os.IsPathSeparator('\\') {
		pathSep = "\\"
	} else {
		pathSep = "/"
	}
}

///////////////////////////////////// HTTP Client Engine Stucture/Class
type Compress struct {
	TargetFolder string // Root Folder
}

func tarGzDir(srcDirPath string, recPath string, tw *tar.Writer, sReplacePrefix string, funcAction func(tarw *tar.Writer, filew *os.File) []byte) bool {
	// Open source diretory
	dir, err := os.Open(srcDirPath)
	if err != nil {
		return false
	}
	defer dir.Close()

	// Get file info slice
	fis, err := dir.Readdir(0)
	if err != nil {
		return false
	}

	for _, fi := range fis {
		// Append path
		curPath := path.Join(srcDirPath, fi.Name())
		// Check it is directory or file
		if fi.IsDir() {
			// Directory
			// (Directory won't add unitl all subfiles are added)
			tarGzDir(curPath, path.Join(recPath, fi.Name()), tw, sReplacePrefix, funcAction)
		}

		tarGzFile(curPath, path.Join(recPath, fi.Name()), tw, fi, sReplacePrefix, funcAction)
	}

	return true
}

func tarGzFile(srcFile string, recPath string, tw *tar.Writer, fi os.FileInfo, sReplacePrefix string, funcAction func(tarw *tar.Writer, filew *os.File) []byte) bool {
	if fi.IsDir() {
		// Create tar header
		hdr := new(tar.Header)
		// if last character of header name is '/' it also can be directory
		// but if you don't set Typeflag, error will occur when you untargz
		hdr.Name = recPath + "/"

		if "MIN5" == sReplacePrefix {
			hdr.Name = strings.Replace(hdr.Name, "MIN/", sReplacePrefix+"/", -1)
		}

		hdr.Typeflag = tar.TypeDir
		hdr.Size = 0
		//hdr.Mode = 0755 | c_ISDIR
		hdr.Mode = int64(fi.Mode())
		hdr.ModTime = fi.ModTime()

		// Write hander
		err := tw.WriteHeader(hdr)
		if err != nil {
			return false
		}
	} else {
		// File reader
		fr, err := os.Open(srcFile)
		if err != nil {
			return false
		}
		defer fr.Close()

		// Create tar header
		hdr := new(tar.Header)
		hdr.Name = recPath

		if "MIN5" == sReplacePrefix {
			hdr.Name = strings.Replace(hdr.Name, "MIN/", sReplacePrefix+"/", -1)
		}

		var bData []byte
		if nil != funcAction {
			bData = funcAction(tw, fr)
			hdr.Size = int64(len(bData))
		} else {
			hdr.Size = fi.Size()
		}
		hdr.Mode = int64(fi.Mode())
		hdr.ModTime = fi.ModTime()

		// Write hander
		err = tw.WriteHeader(hdr)
		if err != nil {
			return false
		}

		if nil == funcAction {
			// Write file data
			_, err = io.Copy(tw, fr)
			if err != nil {
				return false
			}
		} else {
			//funcAction(tw, fr)
			tw.Write(bData)
		}
	}

	return true
}

///////////////////////////////////// [OutterMethod]
// [method] Zip
func (pSelf *Compress) Zip(sResName string, objDataSrc *DataSourceConfig) bool {
	var sZipFile string = ""
	var sDataType string = strings.ToLower(sResName[strings.Index(sResName, "."):])              // data type (d1/m1/m5)
	var sDestFolder string = filepath.Join(pSelf.TargetFolder, strings.ToUpper(objDataSrc.MkID)) // target folder of data(.zip)
	log.Printf("[INF] Compress.Zip() : [Compressing] ExchangeCode:%s, DataType:%s, Folder:%s", objDataSrc.MkID, sDataType, objDataSrc.Folder)

	switch {
	case (objDataSrc.MkID == "sse" && sDataType == ".d1") || (objDataSrc.MkID == "szse" && sDataType == ".d1"):
		sZipFile = filepath.Join(sDestFolder, "DAY.zip")
		if false == pSelf.zipFolder(sZipFile, objDataSrc.Folder) {
			return false
		}
	case (objDataSrc.MkID == "sse" && sDataType == ".m1") || (objDataSrc.MkID == "szse" && sDataType == ".m1"):
		sZipFile = filepath.Join(sDestFolder, "MIN.zip")
		if false == pSelf.zipM1Folder(sZipFile, objDataSrc.Folder) {
			return false
		}
	case (objDataSrc.MkID == "sse" && sDataType == ".m5") || (objDataSrc.MkID == "szse" && sDataType == ".m5"):
		sZipFile = filepath.Join(sDestFolder, "MIN5.zip")
		if false == pSelf.zipM5Folder(sZipFile, objDataSrc.Folder) {
			return false
		}
	default:
		log.Printf("[ERR] Compress.Zip() : [Compressing] invalid exchange code(%s) or data type(%s)", objDataSrc.MkID, sDataType)
		return false
	}

	objDataSrc.Folder = sZipFile
	// get absolute path of URI in local machine
	objFile, err := os.Open(sZipFile)
	if err != nil {
		log.Println("[WARN] Compress.Zip() : local file is not exist :", sZipFile)
		return false
	}

	// parepare 2 generate md5
	defer objFile.Close()
	objMD5Hash := md5.New()
	if _, err := io.Copy(objMD5Hash, objFile); err != nil {
		log.Printf("[WARN] Compress.Zip() : failed 2 generate MD5 : %s : %s", sZipFile, err.Error())
		return false
	}

	// generate MD5 string
	var byteMD5 []byte
	objDataSrc.MD5 = fmt.Sprintf("%x", objMD5Hash.Sum(byteMD5))

	return true
}

///////////////////////////////////// [InnerMethod]
// [method] Zip M5
func (pSelf *Compress) zipM5Folder(sDestFile, sSrcFolder string) bool {
	sMkFolder := path.Dir(sDestFile)
	if "windows" == runtime.GOOS {
		sMkFolder = sDestFile[:strings.LastIndex(sDestFile, pathSep)]
	}
	err := os.MkdirAll(sMkFolder, 0755)
	if err != nil {
		log.Println("[ERR] Compress.zipM5Folder() : cannot build target folder 4 zip file :", path.Dir(sDestFile))
		return false
	}

	objZipFile, err := os.Create(sDestFile)
	objZlibWriter, err := gzip.NewWriterLevel(objZipFile, gzip.BestCompression)
	objZipWriter := tar.NewWriter(objZlibWriter)
	defer objZipFile.Close()
	defer objZlibWriter.Close()
	defer objZipWriter.Close()

	log.Printf("[INF] Compress.zipM5Folder() : zipping (%s) --> (%s)", sSrcFolder, sDestFile)
	if err != nil {
		log.Println("[ERR] Compress.zipM5Folder() : failed 2 create zip file :", sDestFile, err.Error())
		return false
	}

	m5filter := func(tarw *tar.Writer, filew *os.File) []byte {
		var nToday int = time.Now().Year()
		var objMin5 struct {
			Date         int     // date
			Time         int     // time
			Open         float64 // open price
			High         float64 // high price
			Low          float64 // low price
			Close        float64 // close price
			Settle       float64 // settle price
			Amount       float64 // Amount
			Volume       int64   // Volume
			OpenInterest int64   // Open Interest
			NumTrades    int64   // Trade Number
			Voip         float64 // Voip
		} // 5 minutes k-line

		rstr := ""
		bytesData, err := ioutil.ReadAll(filew)
		if err != nil {
			return []byte(rstr)
		}

		bLines := bytes.Split(bytesData, []byte("\n"))
		nCount := len(bLines)
		for i, bLine := range bLines {
			lstRecords := strings.Split(string(bLine), ",")
			if len(lstRecords[0]) <= 0 {
				continue
			}
			objMin5.Date, err = strconv.Atoi(lstRecords[0])
			if err != nil {
				continue
			}

			if (nToday - (objMin5.Date / 10000)) > 1 {
				continue
			}

			// cal. 5 minutes k-lines
			nCurTime, _ := strconv.Atoi(lstRecords[1])
			objMin5.Close, _ = strconv.ParseFloat(lstRecords[5], 64)
			objMin5.Settle, _ = strconv.ParseFloat(lstRecords[6], 64)
			objMin5.Voip, _ = strconv.ParseFloat(lstRecords[11], 64)

			if nCurTime > objMin5.Time { // begin
				if 0 != i {
					rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", objMin5.Date, objMin5.Time, objMin5.Open, objMin5.High, objMin5.Low, objMin5.Close, objMin5.Settle, objMin5.Amount, objMin5.Volume, objMin5.OpenInterest, objMin5.NumTrades, objMin5.Voip)
				}

				objMin5.Time = (5 - nCurTime%5) + nCurTime
				objMin5.Open, _ = strconv.ParseFloat(lstRecords[2], 64)
				objMin5.High, _ = strconv.ParseFloat(lstRecords[3], 64)
				objMin5.Low, _ = strconv.ParseFloat(lstRecords[4], 64)
				objMin5.Amount, _ = strconv.ParseFloat(lstRecords[7], 64)
				objMin5.Volume, _ = strconv.ParseInt(lstRecords[8], 10, 64)
				objMin5.OpenInterest, _ = strconv.ParseInt(lstRecords[9], 10, 64)
				objMin5.NumTrades, _ = strconv.ParseInt(lstRecords[10], 10, 64)
			} else {
				nHigh, _ := strconv.ParseFloat(lstRecords[3], 64)
				nLow, _ := strconv.ParseFloat(lstRecords[4], 64)
				if nHigh > objMin5.High {
					objMin5.High = nHigh
				}
				if nLow > objMin5.Low {
					objMin5.Low = nLow
				}
				nAmount, _ := strconv.ParseFloat(lstRecords[7], 64)
				objMin5.Amount += nAmount
				nVolume, _ := strconv.ParseInt(lstRecords[8], 10, 64)
				objMin5.Volume += nVolume
				nOpenInterest, _ := strconv.ParseInt(lstRecords[9], 10, 64)
				objMin5.OpenInterest += nOpenInterest
				nNumTrades, _ := strconv.ParseInt(lstRecords[10], 10, 64)
				objMin5.NumTrades += nNumTrades
			}

			if i == (nCount - 1) {
				rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", objMin5.Date, objMin5.Time, objMin5.Open, objMin5.High, objMin5.Low, objMin5.Close, objMin5.Settle, objMin5.Amount, objMin5.Volume, objMin5.OpenInterest, objMin5.NumTrades, objMin5.Voip)
			}
		}

		return []byte(rstr)
	}

	if "windows" != runtime.GOOS {
		if false == tarGzDir(sSrcFolder, path.Base(sSrcFolder), objZipWriter, "MIN5", m5filter) {
			return false
		}
	} else {
		lstLastFolder := strings.Split(sSrcFolder, pathSep)
		sRecFolder := lstLastFolder[len(lstLastFolder)-1]
		if "" == sRecFolder {
			sRecFolder = lstLastFolder[len(lstLastFolder)-2]
		}

		if false == tarGzDir(sSrcFolder, sRecFolder, objZipWriter, "MIN5", m5filter) {
			return false
		}
	}

	return true
}

// [method] Zip M1
func (pSelf *Compress) zipM1Folder(sDestFile, sSrcFolder string) bool {
	sMkFolder := path.Dir(sDestFile)
	if "windows" == runtime.GOOS {
		sMkFolder = sDestFile[:strings.LastIndex(sDestFile, pathSep)]
	}
	err := os.MkdirAll(sMkFolder, 0755)
	if err != nil {
		log.Println("[ERR] Compress.zipM1Folder() : cannot build target folder 4 zip file :", path.Dir(sDestFile))
		return false
	}

	objZipFile, err := os.Create(sDestFile)
	objZlibWriter, err := gzip.NewWriterLevel(objZipFile, gzip.BestCompression)
	objZipWriter := tar.NewWriter(objZlibWriter)
	defer objZipFile.Close()
	defer objZlibWriter.Close()
	defer objZipWriter.Close()

	log.Printf("[INF] Compress.zipM1Folder() : zipping (%s) --> (%s)", sSrcFolder, sDestFile)
	if err != nil {
		log.Println("[ERR] Compress.zipM1Folder() : failed 2 create zip file :", sDestFile, err.Error())
		return false
	}

	m1filter := func(tarw *tar.Writer, filew *os.File) []byte {
		nToday := time.Now().Year()*100 + int(time.Now().Month())
		bytesData, err := ioutil.ReadAll(filew)
		rstr := ""

		if err != nil {
			return []byte(rstr)
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

			rstr += (string(bLine) + "\n")
		}

		return []byte(rstr)
	}

	if "windows" != runtime.GOOS {
		if false == tarGzDir(sSrcFolder, path.Base(sSrcFolder), objZipWriter, "", m1filter) {
			return false
		}
	} else {
		lstLastFolder := strings.Split(sSrcFolder, pathSep)
		sRecFolder := lstLastFolder[len(lstLastFolder)-1]
		if "" == sRecFolder {
			sRecFolder = lstLastFolder[len(lstLastFolder)-2]
		}

		if false == tarGzDir(sSrcFolder, sRecFolder, objZipWriter, "", m1filter) {
			return false
		}
	}

	return true
}

// [method] Zip Folder
func (pSelf *Compress) zipFolder(sDestFile, sSrcFolder string) bool {
	sMkFolder := path.Dir(sDestFile)
	if "windows" == runtime.GOOS {
		sMkFolder = sDestFile[:strings.LastIndex(sDestFile, pathSep)]
	}
	err := os.MkdirAll(sMkFolder, 0755)
	if err != nil {
		log.Println("[ERR] Compress.zipFolder() : cannot build target folder 4 zip file :", path.Dir(sDestFile))
		return false
	}

	objZipFile, err := os.Create(sDestFile)
	if nil != err {
		log.Println("[ERR] Compress.zipFolder() : cannot create zip file, ", sDestFile)
	}
	objZlibWriter, err := gzip.NewWriterLevel(objZipFile, gzip.BestCompression)
	if nil != err {
		log.Println("[ERR] Compress.zipFolder() : cannot create gzip file, ")
	}
	objZipWriter := tar.NewWriter(objZlibWriter)
	if nil != err {
		log.Println("[ERR] Compress.zipFolder() : cannot create tar file, ")
	}
	defer objZipFile.Close()
	defer objZlibWriter.Close()
	defer objZipWriter.Close()

	log.Printf("[INF] Compress.zipFolder() : zipping (%s) --> (%s)", sSrcFolder, sDestFile)
	if err != nil {
		log.Println("[ERR] Compress.zipFolder() : failed 2 create zip file :", sDestFile, err.Error())
		return false
	}

	if "windows" != runtime.GOOS {
		if false == tarGzDir(sSrcFolder, path.Base(sSrcFolder), objZipWriter, "", nil) {
			return false
		}
	} else {
		lstLastFolder := strings.Split(sSrcFolder, pathSep)
		sRecFolder := lstLastFolder[len(lstLastFolder)-1]
		if "" == sRecFolder {
			sRecFolder = lstLastFolder[len(lstLastFolder)-2]
		}

		if false == tarGzDir(sSrcFolder, sRecFolder, objZipWriter, "", nil) {
			return false
		}
	}

	return true
}
