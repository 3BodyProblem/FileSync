/**
 * @brief		File's Compressor Tools
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

var sPathSep string = "\\"

// Package Initialization
func init() {
	if os.IsPathSeparator('\\') {
		sPathSep = "\\"
	} else {
		sPathSep = "/"
	}
}

///////////////////////////////////// Data Record IO Wrapper
type CompressHandles struct {
	TarFile    *os.File     // .tar file handle
	GZipWriter *gzip.Writer // gzip.Writer handle
	TarWriter  *tar.Writer  // tar.Writer handle
}

func (pSelf *CompressHandles) OpenFile(sFilePath string) bool {
	var err error

	pSelf.TarFile, err = os.Create(sFilePath)
	if err != nil {
		log.Println("[ERR] CompressHandles.OpenFile() : failed 2 create *.tar file :", sFilePath, err.Error())
		return false
	}

	pSelf.GZipWriter, err = gzip.NewWriterLevel(pSelf.TarFile, gzip.BestCompression)
	if err != nil {
		log.Println("[ERR] CompressHandles.OpenFile() : failed 2 create *tar.Writer :", sFilePath, err.Error())
		return false
	}

	pSelf.TarWriter = tar.NewWriter(pSelf.GZipWriter)
	if err != nil {
		log.Println("[ERR] CompressHandles.OpenFile() : failed 2 create *gzip.Writer :", sFilePath, err.Error())
		return false
	}

	log.Printf("[INF] CompressHandles.OpenFile() : [OK] (%s)", sFilePath)
	return true
}

func (pSelf *CompressHandles) CloseFile() {
	if pSelf.GZipWriter != nil {
		pSelf.GZipWriter.Close()
	}
	if pSelf.TarWriter != nil {
		pSelf.TarWriter.Close()
	}
	if pSelf.TarFile != nil {
		pSelf.TarFile.Close()
		log.Printf("[INF] CompressHandles.CloseFile() : [OK] (%s)", pSelf.TarFile.Name())
	}
}

type I_Record_IO interface {
	Initialize() bool
	Release() []ResDownload
	LoadFromFile(bytesData []byte) ([]byte, int, int)   // load data from file, return [] byte (return nil means end of file)
	GenFilePath(sFileName string) string                // generate name  of file which in .tar
	GrapWriter(sFilePath string, nDate int) *tar.Writer // grap a .tar writer ptr
}

type BaseRecordIO struct {
	mapFileHandle map[string]CompressHandles
}

func (pSelf *BaseRecordIO) Initialize() bool {
	pSelf.mapFileHandle = make(map[string]CompressHandles)
	return true
}

func (pSelf *BaseRecordIO) Release() []ResDownload {
	var byteMD5 []byte
	var lstRes []ResDownload
	log.Println("[INF] BaseRecordIO.Release() : flushing files 2 disk, count =", len(pSelf.mapFileHandle))

	for sPath, objHandles := range pSelf.mapFileHandle {
		objHandles.CloseFile()

		objMd5File, err := os.Open(sPath)
		if err != nil {
			log.Println("[WARN] BaseRecordIO.Release() : local file is not exist :", sPath)
			return lstRes
		}
		defer objMd5File.Close()
		/////////////////////// Generate MD5 String
		objMD5Hash := md5.New()
		if _, err := io.Copy(objMD5Hash, objMd5File); err != nil {
			log.Printf("[WARN] BaseRecordIO.Release() : failed 2 generate MD5 : %s : %s", sPath, err.Error())
			return lstRes
		}

		sMD5 := strings.ToLower(fmt.Sprintf("%x", objMD5Hash.Sum(byteMD5)))
		log.Printf("[INF] BaseRecordIO.Release() : close file = %s, md5 = %s", sPath, sMD5)
		lstRes = append(lstRes, ResDownload{URI: sPath, MD5: sMD5, UPDATE: time.Now().Format("2006-01-02 15:04:05")})
	}

	return lstRes
}

func (pSelf *BaseRecordIO) GenFilePath(sFileName string) string {
	return sFileName
}

func (pSelf *BaseRecordIO) GrapWriter(sFilePath string, nDate int) *tar.Writer {
	var sFile string = fmt.Sprintf("%s%d", sFilePath, nDate)

	if objHandles, ok := pSelf.mapFileHandle[sFile]; ok {
		return objHandles.TarWriter
	} else {
		var objCompressHandles CompressHandles

		if true == objCompressHandles.OpenFile(sFile) {
			pSelf.mapFileHandle[sFile] = objCompressHandles

			return pSelf.mapFileHandle[sFile].TarWriter
		} else {
			log.Println("[ERR] BaseRecordIO.GrapWriter() : failed 2 open *tar.Writer :", sFilePath)
		}

	}

	return nil
}

///////////////////////////////////// Resources Compressor
type Compressor struct {
	TargetFolder string // Root Folder
}

///////////////////////////////////// Private Method
// Compress Folder Recursively
func (pSelf *Compressor) compressFolder(sDestFile string, sSrcFolder string, sRecursivePath string, pILoader I_Record_IO) bool {
	oDirFile, err := os.Open(sSrcFolder) // Open source diretory
	if err != nil {
		return false
	}
	defer oDirFile.Close()

	lstFileInfo, err := oDirFile.Readdir(0) // Get file info slice
	if err != nil {
		return false
	}

	for _, oFileInfo := range lstFileInfo {
		sCurPath := path.Join(sSrcFolder, oFileInfo.Name()) // Append path
		if oFileInfo.IsDir() {                              // Check it is directory or file
			pSelf.compressFolder(sDestFile, sCurPath, path.Join(sRecursivePath, oFileInfo.Name()), pILoader) // (Directory won't add unitl all subfiles are added)
		}

		compressFile(sDestFile, sCurPath, path.Join(sRecursivePath, oFileInfo.Name()), oFileInfo, pILoader)
	}

	return true
}

// Compress A File ( gzip + tar )
func compressFile(sDestFile string, sSrcFile string, sRecursivePath string, oFileInfo os.FileInfo, pILoader I_Record_IO) bool {
	if oFileInfo.IsDir() {
	} else {
		var nIndex int = 0
		oSrcFile, err := os.Open(sSrcFile) // File reader
		if err != nil {
			return false
		}
		defer oSrcFile.Close()
		bytesData, err := ioutil.ReadAll(oSrcFile)
		if err != nil {
			return false
		}

		for {
			hdr := new(tar.Header) // Create tar header
			hdr.Name = pILoader.GenFilePath(sRecursivePath)
			bData, nDate, nOffset := pILoader.LoadFromFile(bytesData[nIndex:])
			if len(bData) <= 0 {
				break
			}

			nIndex += nOffset
			pTarWriter := pILoader.GrapWriter(sDestFile, nDate)
			if nil == pTarWriter {
				return false
			}

			hdr.Size = int64(len(bData))
			hdr.Mode = int64(oFileInfo.Mode())
			hdr.ModTime = oFileInfo.ModTime()
			err = pTarWriter.WriteHeader(hdr) // Write hander
			if err != nil {
				return false
			}

			pTarWriter.Write(bData) // Write file data
		}
	}

	return true
}

///////////////////////////////////// [OutterMethod]
// [method] XCompress
func (pSelf *Compressor) XCompress(sResName string, objDataSrc *DataSourceConfig) ([]ResDownload, bool) {
	var lstRes []ResDownload
	var sDataType string = strings.ToLower(sResName[strings.Index(sResName, "."):])              // data type (d1/m1/m5/wt)
	var sDestFolder string = filepath.Join(pSelf.TargetFolder, strings.ToUpper(objDataSrc.MkID)) // target folder of data(.tar.gz)

	sDestFolder = strings.Replace(sDestFolder, "\\", "/", -1)
	log.Printf("[INF] Compressor.XCompress() : [Compressing] ExchangeCode:%s, DataType:%s, Folder:%s", objDataSrc.MkID, sDataType, objDataSrc.Folder)

	switch {
	case (objDataSrc.MkID == "sse" && sDataType == ".wt") || (objDataSrc.MkID == "szse" && sDataType == ".wt"):
		var objRecordIO WeightRecordIO // policy of Weight data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "WEIGHT/WEIGHT."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".d1") || (objDataSrc.MkID == "szse" && sDataType == ".d1"):
		var objRecordIO Day1RecordIO // policy of Day data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "DAY/DAY."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".m1") || (objDataSrc.MkID == "szse" && sDataType == ".m1"):
		var objRecordIO Minutes1RecordIO // policy of M1 data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "MIN/MIN."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".m5") || (objDataSrc.MkID == "szse" && sDataType == ".m5"):
		var objRecordIO Minutes5RecordIO // policy of M5 data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "MIN5/MIN5."), objDataSrc.Folder, &objRecordIO)
	default:
		log.Printf("[ERR] Compressor.XCompress() : [Compressing] invalid exchange code(%s) or data type(%s)", objDataSrc.MkID, sDataType)
		return lstRes, false
	}
}

///////////////////////////////////// [InnerMethod]
// [Method] load source data 2 targer folder
func (pSelf *Compressor) translateFolder(sDestFile, sSrcFolder string, pILoader I_Record_IO) ([]ResDownload, bool) {
	var lstRes []ResDownload
	var sMkFolder string = path.Dir(sDestFile)
	//////////////// Prepare Data Folder && File Handles
	if "windows" == runtime.GOOS {
		sMkFolder = sDestFile[:strings.LastIndex(sDestFile, pathSep)]
	}
	sDestFile = strings.Replace(sDestFile, "\\", "/", -1)
	err := os.MkdirAll(sMkFolder, 0755)
	if err != nil {
		log.Println("[ERR] Compressor.translateFolder() : cannot build target folder 4 zip file :", sMkFolder)
		return lstRes, false
	}
	///////////////// Initialize Object type(I_Record_IO)
	log.Printf("[INF] Compressor.translateFolder() : compressing ---> (%s)", sSrcFolder)
	if false == pILoader.Initialize() {
		log.Println("[ERR] Compressor.translateFolder() : Cannot initialize I_Record_IO object, ", sSrcFolder)
		return lstRes, false
	}
	///////////////// Compressing Source Data Folder
	if "windows" != runtime.GOOS {
		if false == pSelf.compressFolder(sDestFile, sSrcFolder, path.Base(sSrcFolder), pILoader) {
			return lstRes, false
		}
	} else {
		if false == pSelf.compressFolder(sDestFile, sSrcFolder, "./", pILoader) {
			return lstRes, false
		}
	}

	return pILoader.Release(), true
}

///////////////////////// 5Minutes Lines
type Minutes5RecordIO struct {
	BaseRecordIO
}

func (pSelf *Minutes5RecordIO) GenFilePath(sFileName string) string {
	return strings.Replace(sFileName, "MIN/", "MIN5/", -1)
}

func (pSelf *Minutes5RecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	var err error
	var nOffset int = 0
	var bLine []byte
	var i int = 0
	var nReturnDate int = -100
	var rstr string = ""
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

	bLines := bytes.Split(bytesData, []byte("\n"))
	nCount := len(bLines)
	for i, bLine = range bLines {
		nOffset += (len(bLine) + 1)
		lstRecords := strings.Split(string(bLine), ",")
		if len(lstRecords[0]) <= 0 {
			continue
		}
		objMin5.Date, err = strconv.Atoi(lstRecords[0])
		if err != nil {
			continue
		}

		if -100 == nReturnDate {
			nReturnDate = objMin5.Date
		}

		if nReturnDate != objMin5.Date {
			return []byte(rstr), nReturnDate, nOffset
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

	return []byte(rstr), nReturnDate, len(bytesData)
}

///////////////////////// 1Minutes Lines
type Minutes1RecordIO struct {
	BaseRecordIO
}

func (pSelf *Minutes1RecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	var nReturnDate int = -100
	var rstr string = ""
	var nOffset int = 0

	for _, bLine := range bytes.Split(bytesData, []byte("\n")) {
		nOffset += (len(bLine) + 1)
		sFirstFields := strings.Split(string(bLine), ",")[0]
		if len(sFirstFields) <= 0 {
			continue
		}
		nDate, err := strconv.Atoi(sFirstFields)
		if err != nil {
			continue
		}

		if -100 == nReturnDate {
			nReturnDate = nDate
		}

		if nReturnDate != nDate {
			return []byte(rstr), nReturnDate, nOffset
		}

		rstr += (string(bLine) + "\n")
	}

	return []byte(rstr), nReturnDate, len(bytesData)
}

///////////////////////// 1 Day Lines
type Day1RecordIO struct {
	BaseRecordIO
}

func (pSelf *Day1RecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	var nReturnDate int = -100
	var rstr string = ""
	var nOffset int = 0

	for _, bLine := range bytes.Split(bytesData, []byte("\n")) {
		nOffset += (len(bLine) + 1)
		sFirstFields := strings.Split(string(bLine), ",")[0]
		if len(sFirstFields) <= 0 {
			continue
		}
		nDate, err := strconv.Atoi(sFirstFields)
		if err != nil {
			continue
		}

		if -100 == nReturnDate {
			nReturnDate = nDate
		}

		if nReturnDate != nDate {
			return []byte(rstr), nReturnDate, nOffset
		}

		rstr += (string(bLine) + "\n")
	}

	return []byte(rstr), nReturnDate, len(bytesData)
}

///////////////////////// Weights Lines
type WeightRecordIO struct {
	BaseRecordIO
}

func (pSelf *WeightRecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 0, len(bytesData)
}
