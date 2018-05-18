/**
 * @brief		File's Compressor Tools
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"archive/tar"
	"bytes"
	"compress/zlib"
	"crypto/md5"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Package Initialization
func init() {
}

///////////////////////////////////// Data Record IO Wrapper ///////////////////////////////////////////
type CompressHandles struct {
	TarFile    *os.File     // .tar file handle
	GZipWriter *zlib.Writer // gzip.Writer handle
	TarWriter  *tar.Writer  // tar.Writer handle
}

func (pSelf *CompressHandles) OpenFile(sFilePath string, nGZipCompressLevel int) bool {
	var err error

	pSelf.TarFile, err = os.Create(sFilePath)
	if err != nil {
		log.Println("[ERR] CompressHandles.OpenFile() : failed 2 create *.tar file :", sFilePath, err.Error())
		return false
	}

	pSelf.GZipWriter, err = zlib.NewWriterLevel(pSelf.TarFile, nGZipCompressLevel)
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
	LoadFromFile(bytesData []byte) ([]byte, int, int)                    // load data from file, return [] byte (return nil means end of file)
	CodeInWhiteTable(sFileName string) bool                              // judge whether the file need 2 be loaded
	GenFilePath(sFileName string) string                                 // generate name  of file which in .tar
	GrapWriter(sFilePath string, nDate int, sSrcFile string) *tar.Writer // grap a .tar writer ptr
	GetCompressLevel() int                                               // get gzip compression level
}

type BaseRecordIO struct {
	DataType        string
	CodeRangeFilter I_Range_OP
	mapFileHandle   map[string]CompressHandles
}

func (pSelf *BaseRecordIO) GetCompressLevel() int {
	return zlib.DefaultCompression
}

func (pSelf *BaseRecordIO) CodeInWhiteTable(sFileName string) bool {
	return true
}

func (pSelf *BaseRecordIO) Initialize() bool {
	pSelf.mapFileHandle = make(map[string]CompressHandles)
	return true
}

func (pSelf *BaseRecordIO) Release() []ResDownload {
	var byteMD5 []byte
	var lstRes []ResDownload
	var lstSortKeys []string
	log.Println("[INF] BaseRecordIO.Release() : flushing files 2 disk, count =", len(pSelf.mapFileHandle))

	for sPath, objHandles := range pSelf.mapFileHandle {
		objHandles.CloseFile()
		lstSortKeys = append(lstSortKeys, sPath)
	}

	sort.Strings(lstSortKeys)
	for _, sVal := range lstSortKeys {
		objMd5File, err := os.Open(sVal)
		if err != nil {
			log.Println("[WARN] BaseRecordIO.Release() : local file is not exist :", sVal)
			return lstRes
		}
		defer objMd5File.Close()
		/////////////////////// Generate MD5 String
		objMD5Hash := md5.New()
		if _, err := io.Copy(objMD5Hash, objMd5File); err != nil {
			log.Printf("[WARN] BaseRecordIO.Release() : failed 2 generate MD5 : %s : %s", sVal, err.Error())
			return lstRes
		}

		sMD5 := strings.ToLower(fmt.Sprintf("%x", objMD5Hash.Sum(byteMD5)))
		log.Printf("[INF] BaseRecordIO.Release() : close file = %s, md5 = %s", sVal, sMD5)
		lstRes = append(lstRes, ResDownload{TYPE: pSelf.DataType, URI: sVal, MD5: sMD5, UPDATE: time.Now().Format("2006-01-02 15:04:05")})
	}

	return lstRes
}

func (pSelf *BaseRecordIO) GenFilePath(sFileName string) string {
	return sFileName
}

func (pSelf *BaseRecordIO) GrapWriter(sFilePath string, nDate int, sSrcFile string) *tar.Writer {
	var sFile string = ""
	var objToday time.Time = time.Now()

	objRecordDate := time.Date(nDate/10000, time.Month(nDate%10000/100), nDate%100, 21, 6, 9, 0, time.Local)
	subHours := objToday.Sub(objRecordDate)
	nDays := subHours.Hours() / 24

	if nDays <= 16 { ////// Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate)
	} else { ////////////////////////// Not Current Month
		nDD := (nDate % 100) ////////// One File With 2 Week's Data Inside
		if nDD <= 15 {
			nDD = 0
		} else {
			nDD = 15
		}
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate/100*100+nDD)
	}

	if objHandles, ok := pSelf.mapFileHandle[sFile]; ok {
		return objHandles.TarWriter
	} else {
		var objCompressHandles CompressHandles

		if true == objCompressHandles.OpenFile(sFile, pSelf.GetCompressLevel()) {
			pSelf.mapFileHandle[sFile] = objCompressHandles

			return pSelf.mapFileHandle[sFile].TarWriter
		} else {
			log.Println("[ERR] BaseRecordIO.GrapWriter() : failed 2 open *tar.Writer :", sFilePath)
		}

	}

	return nil
}

///////////////////////////////////// Resources Compressor ///////////////////////////////////////////
type Compressor struct {
	TargetFolder string // Root Folder
}

///////////////////////////////////// Private Method ///////////////////////////////////////////
// Compress Folder Recursively
func (pSelf *Compressor) compressFolder(sDestFile string, sSrcFolder string, sRecursivePath string, pILoader I_Record_IO) bool {
	oDirFile, err := os.Open(sSrcFolder) // Open source diretory
	if err != nil {
		log.Println("[INF] Compressor.compressFolder() : cannot open source folder :", sSrcFolder, err.Error())
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
		// Code File Is Not In White Table
		if pILoader.CodeInWhiteTable(sSrcFile) == false {
			return true
		}

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

		nDataLen := len(bytesData)
		for nIndex < nDataLen {
			hdr := new(tar.Header) // Create tar header
			//hdr, err := tar.FileInfoHeader(oFileInfo, "")
			hdr.Name = pILoader.GenFilePath(sRecursivePath)
			bData, nDate, nOffset := pILoader.LoadFromFile(bytesData[nIndex:])
			nIndex += nOffset
			// log.Printf("pos:%d(%d) >= len:%d", nIndex, nOffset, nDataLen)
			pTarWriter := pILoader.GrapWriter(pILoader.GenFilePath(sDestFile), nDate, sSrcFile)
			if nil == pTarWriter {
				return false
			}

			hdr.Size = int64(len(bData))
			hdr.Mode = int64(oFileInfo.Mode())
			hdr.ModTime = oFileInfo.ModTime()
			err = pTarWriter.WriteHeader(hdr) // Write hander
			if err != nil {
				log.Println("[INF] Compressor.compressFile() : cannot write tar header 2 file :", sDestFile, err.Error())
				return false
			}

			pTarWriter.Write(bData) // Write file data
		}
	}

	return true
}

///////////////////////////////////// [OutterMethod] ///////////////////////////////////////////
// [method] XCompress
func (pSelf *Compressor) XCompress(sResType string, objDataSrc *DataSourceConfig, codeRange I_Range_OP) ([]ResDownload, bool) {
	var lstRes []ResDownload
	var sDataType string = strings.ToLower(sResType[strings.Index(sResType, "."):])              // data type (d1/m1/m5/wt)
	var sDestFolder string = filepath.Join(pSelf.TargetFolder, strings.ToUpper(objDataSrc.MkID)) // target folder of data(.tar.gz)

	sDestFolder = strings.Replace(sDestFolder, "\\", "/", -1)
	log.Printf("[INF] Compressor.XCompress() : [Compressing] ExchangeCode:%s, DataType:%s, Folder:%s", objDataSrc.MkID, sDataType, objDataSrc.Folder)

	switch {
	case (objDataSrc.MkID == "sse" && sDataType == ".wt") || (objDataSrc.MkID == "szse" && sDataType == ".wt"):
		objRecordIO := WeightRecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of Weight data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "WEIGHT/WEIGHT."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".d1") || (objDataSrc.MkID == "szse" && sDataType == ".d1"):
		objRecordIO := Day1RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of Day data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "DAY/DAY."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".m1") || (objDataSrc.MkID == "szse" && sDataType == ".m1"):
		objRecordIO := Minutes1RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of M1 data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "MIN/MIN."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".m5") || (objDataSrc.MkID == "szse" && sDataType == ".m5"):
		objRecordIO := Minutes5RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of M5 data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "MIN5/MIN5."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".m60") || (objDataSrc.MkID == "szse" && sDataType == ".m60"):
		objRecordIO := Minutes60RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of M60 data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "MIN60/MIN60."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".participant":
		objRecordIO := ParticipantRecordIO{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "Participant."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".shase_rzrq_by_date":
		objRecordIO := Shase_rzrq_by_date{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "shase_rzrq_by_date/shase_rzrq_by_date."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".sznse_rzrq_by_date":
		objRecordIO := Sznse_rzrq_by_date{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "sznse_rzrq_by_date/sznse_rzrq_by_date."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".shsz_idx_by_date":
		objRecordIO := Shsz_idx_by_date{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "shsz_idx_by_date/shsz_idx_by_date."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".shsz_detail":
		objRecordIO := Shsz_detail{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.translateFolder(filepath.Join(sDestFolder, "shsz_detail/shsz_detail."), objDataSrc.Folder, &objRecordIO)
	default:
		log.Printf("[ERR] Compressor.XCompress() : [Compressing] invalid exchange code(%s) or data type(%s)", objDataSrc.MkID, sDataType)
		return lstRes, false
	}
}

///////////////////////////////////// [InnerMethod] ///////////////////////////////////////////
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
	sDestFile = pILoader.GenFilePath(sDestFile)
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

///////////////////////// 60Minutes Lines ///////////////////////////////////////////
type Minutes60RecordIO struct {
	BaseRecordIO
}

func (pSelf *Minutes60RecordIO) CodeInWhiteTable(sFileName string) bool {
	if pSelf.CodeRangeFilter == nil {
		return true
	}

	nEnd := strings.LastIndexAny(sFileName, ".")
	nFileYear, err := strconv.Atoi(sFileName[nEnd-4 : nEnd])
	if nil != err {
		log.Println("[ERR] Minutes1RecordIO.CodeInWhiteTable() : Year In FileName is not digital: ", sFileName, nFileYear)
		return false
	}
	if time.Now().Year()-nFileYear >= 4 {
		return false
	}
	nBegin := strings.LastIndexAny(sFileName, "MIN")
	nEnd = nEnd - 5
	sCodeNum := sFileName[nBegin+1 : nEnd]

	return pSelf.CodeRangeFilter.CodeInRange(sCodeNum)
}

func (pSelf *Minutes60RecordIO) GenFilePath(sFileName string) string {
	return strings.Replace(sFileName, "MIN/", "MIN60/", -1)
}

func (pSelf *Minutes60RecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	var err error
	var nOffset int = 0
	var nReturnDate int = -100
	var objToday time.Time = time.Now()
	var rstr string = ""
	var lstPeriods = [4]int{103000, 130000, 140000, 150000}
	var nLastIndex int = -1
	var nCurIndex int = 0
	var objMin60 struct {
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
	} // 60 minutes k-line

	bNewBegin := true
	bLines := bytes.Split(bytesData, []byte("\n"))
	for _, bLine := range bLines {
		nOffset += (len(bLine) + 1)
		lstRecords := strings.Split(string(bLine), ",")
		if len(lstRecords[0]) <= 0 {
			continue
		}
		objMin60.Date, err = strconv.Atoi(lstRecords[0])
		if err != nil {
			continue
		}

		objRecordDate := time.Date(objMin60.Date/10000, time.Month(objMin60.Date%10000/100), objMin60.Date%100, 21, 6, 9, 0, time.Local)
		subHours := objToday.Sub(objRecordDate)
		nDays := subHours.Hours() / 24
		if nDays > 366*3 {
			continue
		}

		if -100 == nReturnDate {
			nReturnDate = objMin60.Date
		}

		// cal. 60 minutes k-lines
		nCurTime, _ := strconv.Atoi(lstRecords[1])
		objMin60.Close, _ = strconv.ParseFloat(lstRecords[5], 64)
		objMin60.Settle, _ = strconv.ParseFloat(lstRecords[6], 64)
		objMin60.Voip, _ = strconv.ParseFloat(lstRecords[11], 64)

		nPeriodTime := 0
		if nCurTime >= 63000 && nCurTime < 103000 {
			nCurIndex = 0
			nPeriodTime = 103000
		} else if nCurTime >= 103000 && nCurTime <= 113000 {
			nCurIndex = 1
			nPeriodTime = 130000
		} else if nCurTime > 113000 && nCurTime < 140000 {
			nCurIndex = 2
			nPeriodTime = 140000
		} else if nCurTime >= 140000 && nCurTime <= 160000 {
			nCurIndex = 3
			nPeriodTime = 150000
		} else {
			continue
		}

		if nReturnDate != objMin60.Date {
			bNewBegin = false
			rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", objMin60.Date, objMin60.Time, objMin60.Open, objMin60.High, objMin60.Low, objMin60.Close, objMin60.Settle, objMin60.Amount, objMin60.Volume, objMin60.OpenInterest, objMin60.NumTrades, objMin60.Voip)
			return []byte(rstr), nReturnDate, nOffset
		}

		if nLastIndex != nCurIndex {
			nLastIndex = nCurIndex

			if 0 == objMin60.Time {
				objMin60.Time = lstPeriods[0]
				if nCurIndex > 0 {
					objMin60.Time = lstPeriods[nCurIndex-1]
				}
			}

			if nCurIndex > 0 {
				bNewBegin = false
				rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", objMin60.Date, objMin60.Time, objMin60.Open, objMin60.High, objMin60.Low, objMin60.Close, objMin60.Settle, objMin60.Amount, objMin60.Volume, objMin60.OpenInterest, objMin60.NumTrades, objMin60.Voip)
			}

			bNewBegin = true
			objMin60.Time = nPeriodTime
			objMin60.Open = objMin60.Close
			objMin60.High = objMin60.Close
			objMin60.Low = objMin60.Close
			objMin60.Amount, _ = strconv.ParseFloat(lstRecords[7], 64)
			objMin60.Volume, _ = strconv.ParseInt(lstRecords[8], 10, 64)
			objMin60.OpenInterest, _ = strconv.ParseInt(lstRecords[9], 10, 64)
			objMin60.NumTrades, _ = strconv.ParseInt(lstRecords[10], 10, 64)
		} else {
			objMin60.Time = nPeriodTime
			if objMin60.Close > objMin60.High {
				objMin60.High = objMin60.Close
			}

			if objMin60.Close < objMin60.Low {
				objMin60.Low = objMin60.Close
			}

			nAmount, _ := strconv.ParseFloat(lstRecords[7], 64)
			objMin60.Amount += nAmount
			nVolume, _ := strconv.ParseInt(lstRecords[8], 10, 64)
			objMin60.Volume += nVolume
			nOpenInterest, _ := strconv.ParseInt(lstRecords[9], 10, 64)
			objMin60.OpenInterest += nOpenInterest
			nNumTrades, _ := strconv.ParseInt(lstRecords[10], 10, 64)
			objMin60.NumTrades += nNumTrades
		}
	}

	if true == bNewBegin {
		if objMin60.Time > 0 {
			rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", objMin60.Date, objMin60.Time, objMin60.Open, objMin60.High, objMin60.Low, objMin60.Close, objMin60.Settle, objMin60.Amount, objMin60.Volume, objMin60.OpenInterest, objMin60.NumTrades, objMin60.Voip)
		}
	}

	return []byte(rstr), nReturnDate, len(bytesData)
}

///////////////////////// 5Minutes Lines ///////////////////////////////////////////
type Minutes5RecordIO struct {
	BaseRecordIO
}

func (pSelf *Minutes5RecordIO) CodeInWhiteTable(sFileName string) bool {
	if pSelf.CodeRangeFilter == nil {
		return true
	}

	nEnd := strings.LastIndexAny(sFileName, ".")
	nFileYear, err := strconv.Atoi(sFileName[nEnd-4 : nEnd])
	if nil != err {
		log.Println("[ERR] Minutes1RecordIO.CodeInWhiteTable() : Year In FileName is not digital: ", sFileName, nFileYear)
		return false
	}
	if time.Now().Year()-nFileYear >= 2 {
		return false
	}
	nBegin := strings.LastIndexAny(sFileName, "MIN")
	nEnd = nEnd - 5
	sCodeNum := sFileName[nBegin+1 : nEnd]

	return pSelf.CodeRangeFilter.CodeInRange(sCodeNum)
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
	var objToday time.Time = time.Now()
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

		objRecordDate := time.Date(objMin5.Date/10000, time.Month(objMin5.Date%10000/100), objMin5.Date%100, 21, 6, 9, 0, time.Local)
		subHours := objToday.Sub(objRecordDate)
		nDays := subHours.Hours() / 24
		if nDays > 366 {
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
		nCurTime /= 100000
		objMin5.Close, _ = strconv.ParseFloat(lstRecords[5], 64)
		objMin5.Settle, _ = strconv.ParseFloat(lstRecords[6], 64)
		objMin5.Voip, _ = strconv.ParseFloat(lstRecords[11], 64)

		if objMin5.Time == 0 {
			objMin5.Time = (nCurTime + 5) * 100
			objMin5.Open, _ = strconv.ParseFloat(lstRecords[2], 64)
			objMin5.High, _ = strconv.ParseFloat(lstRecords[3], 64)
			objMin5.Low, _ = strconv.ParseFloat(lstRecords[4], 64)
			objMin5.Amount, _ = strconv.ParseFloat(lstRecords[7], 64)
			objMin5.Volume, _ = strconv.ParseInt(lstRecords[8], 10, 64)
			objMin5.OpenInterest, _ = strconv.ParseInt(lstRecords[9], 10, 64)
			objMin5.NumTrades, _ = strconv.ParseInt(lstRecords[10], 10, 64)
			rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", objMin5.Date, objMin5.Time, objMin5.Open, objMin5.High, objMin5.Low, objMin5.Close, objMin5.Settle, objMin5.Amount, objMin5.Volume, objMin5.OpenInterest, objMin5.NumTrades, objMin5.Voip)
		}

		if objMin5.Time <= nCurTime*100 { // begin
			//if 0 != i {
			rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", objMin5.Date, objMin5.Time, objMin5.Open, objMin5.High, objMin5.Low, objMin5.Close, objMin5.Settle, objMin5.Amount, objMin5.Volume, objMin5.OpenInterest, objMin5.NumTrades, objMin5.Voip)
			//}

			objMin5.Time = (nCurTime + 5) * 100
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

///////////////////////// 1Minutes Lines ///////////////////////////////////////////
type Minutes1RecordIO struct {
	BaseRecordIO
}

func (pSelf *Minutes1RecordIO) CodeInWhiteTable(sFileName string) bool {
	if pSelf.CodeRangeFilter == nil {
		return true
	}

	nEnd := strings.LastIndexAny(sFileName, ".")
	nFileYear, err := strconv.Atoi(sFileName[nEnd-4 : nEnd])
	if nil != err {
		log.Println("[ERR] Minutes1RecordIO.CodeInWhiteTable() : Year In FileName is not digital: ", sFileName, nFileYear)
		return false
	}
	if time.Now().Year()-nFileYear >= 2 {
		return false
	}
	nBegin := strings.LastIndexAny(sFileName, "MIN")
	nEnd = nEnd - 5
	sCodeNum := sFileName[nBegin+1 : nEnd]

	return pSelf.CodeRangeFilter.CodeInRange(sCodeNum)
}

func (pSelf *Minutes1RecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	var nReturnDate int = -100
	var rstr string = ""
	var nOffset int = 0
	var objToday time.Time = time.Now()

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

		objRecordDate := time.Date(nDate/10000, time.Month(nDate%10000/100), nDate%100, 21, 6, 9, 0, time.Local)
		subHours := objToday.Sub(objRecordDate)
		nDays := subHours.Hours() / 24
		if nDays > 14 {
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

///////////////////////// 1 Day Lines ///////////////////////////////////////////
type Day1RecordIO struct {
	BaseRecordIO
}

func (pSelf *Day1RecordIO) GetCompressLevel() int {
	return zlib.BestSpeed
}

func (pSelf *Day1RecordIO) GrapWriter(sFilePath string, nDate int, sSrcFile string) *tar.Writer {
	var sFile string = ""
	var objToday time.Time = time.Now()

	objRecordDate := time.Date(nDate/10000, time.Month(nDate%10000/100), nDate%100, 21, 6, 9, 0, time.Local)
	subHours := objToday.Sub(objRecordDate)
	nDays := subHours.Hours() / 24

	if nDays <= 16 { ////// Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate)
	} else { ////////////////////////// Not Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate/10000*10000)
	}

	if objHandles, ok := pSelf.mapFileHandle[sFile]; ok {
		return objHandles.TarWriter
	} else {
		var objCompressHandles CompressHandles

		if true == objCompressHandles.OpenFile(sFile, pSelf.GetCompressLevel()) {
			pSelf.mapFileHandle[sFile] = objCompressHandles

			return pSelf.mapFileHandle[sFile].TarWriter
		} else {
			log.Println("[ERR] Day1RecordIO.GrapWriter() : failed 2 open *tar.Writer :", sFilePath)
		}

	}

	return nil
}

func (pSelf *Day1RecordIO) CodeInWhiteTable(sFileName string) bool {
	if pSelf.CodeRangeFilter == nil {
		return true
	}

	nBegin := strings.LastIndexAny(sFileName, "DAY")
	nEnd := strings.LastIndexAny(sFileName, ".")
	sCodeNum := sFileName[nBegin+1 : nEnd]

	return pSelf.CodeRangeFilter.CodeInRange(sCodeNum)
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

///////////////////////// Weights Lines ///////////////////////////////////////////
type WeightRecordIO struct {
	BaseRecordIO
}

func (pSelf *WeightRecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 0, len(bytesData)
}

///////////////////////// Participant Lines ///////////////////////////////////////////
type ParticipantRecordIO struct {
	BaseRecordIO
}

func (pSelf *ParticipantRecordIO) CodeInWhiteTable(sFileName string) bool {
	sTmpName := strings.ToLower(sFileName)

	if strings.Contains(sTmpName, "participant.txt") == true {
		return true
	}

	return false
}

func (pSelf *ParticipantRecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 0, len(bytesData)
}

///////////////////////// shase_rzrq_by_date Lines ///////////////////////////////////////////
type Shase_rzrq_by_date struct {
	BaseRecordIO
}

func (pSelf *Shase_rzrq_by_date) CodeInWhiteTable(sFileName string) bool {
	return true
}

func (pSelf *Shase_rzrq_by_date) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 0, len(bytesData)
}

func (pSelf *Shase_rzrq_by_date) GrapWriter(sFilePath string, nDate int, sSrcFile string) *tar.Writer {
	var err error
	var sFile string = ""
	var objToday time.Time = time.Now()

	lstPath := strings.Split(sSrcFile, "/")
	lstName := strings.Split(lstPath[len(lstPath)-1], ".")
	nDate, err = strconv.Atoi(lstName[0])
	if nil != err {
		log.Println("[ERROR] Shase_rzrq_by_date.GrapWriter() : invalid number string: ", lstName[0], err.Error())
		return nil
	}

	objRecordDate := time.Date(nDate/10000, time.Month(nDate%10000/100), nDate%100, 21, 6, 9, 0, time.Local)
	subHours := objToday.Sub(objRecordDate)
	nDays := subHours.Hours() / 24
	if nDays <= 16 { ////// Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate)
	} else { ////////////////////////// Not Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate/10000*10000)
	}

	if objHandles, ok := pSelf.mapFileHandle[sFile]; ok {
		return objHandles.TarWriter
	} else {
		var objCompressHandles CompressHandles

		if true == objCompressHandles.OpenFile(sFile, pSelf.GetCompressLevel()) {
			pSelf.mapFileHandle[sFile] = objCompressHandles

			return pSelf.mapFileHandle[sFile].TarWriter
		} else {
			log.Println("[ERR] Shase_rzrq_by_date.GrapWriter() : failed 2 open *tar.Writer :", sFilePath)
		}

	}

	return nil
}

///////////////////////// sznse_rzrq_by_date Lines ///////////////////////////////////////////
type Sznse_rzrq_by_date struct {
	BaseRecordIO
}

func (pSelf *Sznse_rzrq_by_date) CodeInWhiteTable(sFileName string) bool {
	return true
}

func (pSelf *Sznse_rzrq_by_date) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 0, len(bytesData)
}

func (pSelf *Sznse_rzrq_by_date) GrapWriter(sFilePath string, nDate int, sSrcFile string) *tar.Writer {
	var err error
	var sFile string = ""
	var objToday time.Time = time.Now()

	lstPath := strings.Split(sSrcFile, "/")
	log.Println(sSrcFile, lstPath[0], lstPath[1])
	lstName := strings.Split(lstPath[len(lstPath)-1], ".")
	nDate, err = strconv.Atoi(lstName[0])
	if nil != err {
		log.Println("[ERROR] Sznse_rzrq_by_date.GrapWriter() : invalid number string: ", lstName[0], err.Error())
		return nil
	}

	objRecordDate := time.Date(nDate/10000, time.Month(nDate%10000/100), nDate%100, 21, 6, 9, 0, time.Local)
	subHours := objToday.Sub(objRecordDate)
	nDays := subHours.Hours() / 24
	if nDays <= 16 { ////// Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate)
	} else { ////////////////////////// Not Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate/10000*10000)
	}

	if objHandles, ok := pSelf.mapFileHandle[sFile]; ok {
		return objHandles.TarWriter
	} else {
		var objCompressHandles CompressHandles

		if true == objCompressHandles.OpenFile(sFile, pSelf.GetCompressLevel()) {
			pSelf.mapFileHandle[sFile] = objCompressHandles

			return pSelf.mapFileHandle[sFile].TarWriter
		} else {
			log.Println("[ERR] Sznse_rzrq_by_date.GrapWriter() : failed 2 open *tar.Writer :", sFilePath)
		}

	}

	return nil
}

///////////////////////// shsz_idx_by_date Lines ///////////////////////////////////////////
type Shsz_idx_by_date struct {
	BaseRecordIO
}

func (pSelf *Shsz_idx_by_date) CodeInWhiteTable(sFileName string) bool {
	return true
}

func (pSelf *Shsz_idx_by_date) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 0, len(bytesData)
}

func (pSelf *Shsz_idx_by_date) GrapWriter(sFilePath string, nDate int, sSrcFile string) *tar.Writer {
	var err error
	var sFile string = ""
	var objToday time.Time = time.Now()

	lstPath := strings.Split(sSrcFile, "/")
	lstName := strings.Split(lstPath[len(lstPath)-1], ".")
	nDate, err = strconv.Atoi(lstName[0])
	if nil != err {
		log.Println("[ERROR] Shsz_idx_by_date.GrapWriter() : invalid number string: ", lstName[0], err.Error())
		return nil
	}

	objRecordDate := time.Date(nDate/10000, time.Month(nDate%10000/100), nDate%100, 21, 6, 9, 0, time.Local)
	subHours := objToday.Sub(objRecordDate)
	nDays := subHours.Hours() / 24
	if nDays <= 16 { ////// Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate)
	} else { ////////////////////////// Not Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate/10000*10000)
	}

	if objHandles, ok := pSelf.mapFileHandle[sFile]; ok {
		return objHandles.TarWriter
	} else {
		var objCompressHandles CompressHandles

		if true == objCompressHandles.OpenFile(sFile, pSelf.GetCompressLevel()) {
			pSelf.mapFileHandle[sFile] = objCompressHandles

			return pSelf.mapFileHandle[sFile].TarWriter
		} else {
			log.Println("[ERR] Shsz_idx_by_date.GrapWriter() : failed 2 open *tar.Writer :", sFilePath)
		}

	}

	return nil
}

///////////////////////// shsz_detail Lines ///////////////////////////////////////////
type Shsz_detail struct {
	BaseRecordIO
}

func (pSelf *Shsz_detail) CodeInWhiteTable(sFileName string) bool {
	return true
}

func (pSelf *Shsz_detail) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 0, len(bytesData)
}

func (pSelf *Shsz_detail) GrapWriter(sFilePath string, nDate int, sSrcFile string) *tar.Writer {
	var err error
	var sFile string = ""
	var objToday time.Time = time.Now()

	lstPath := strings.Split(sSrcFile, "/")
	lstName := strings.Split(lstPath[len(lstPath)-1], ".")
	nDate, err = strconv.Atoi(lstName[0])
	if nil != err {
		log.Println("[ERROR] Shsz_detail.GrapWriter() : invalid number string: ", lstName[0], err.Error())
		return nil
	}

	objRecordDate := time.Date(nDate/10000, time.Month(nDate%10000/100), nDate%100, 21, 6, 9, 0, time.Local)
	subHours := objToday.Sub(objRecordDate)
	nDays := subHours.Hours() / 24
	if nDays <= 16 { ////// Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate)
	} else { ////////////////////////// Not Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate/10000*10000)
	}

	if objHandles, ok := pSelf.mapFileHandle[sFile]; ok {
		return objHandles.TarWriter
	} else {
		var objCompressHandles CompressHandles

		if true == objCompressHandles.OpenFile(sFile, pSelf.GetCompressLevel()) {
			pSelf.mapFileHandle[sFile] = objCompressHandles

			return pSelf.mapFileHandle[sFile].TarWriter
		} else {
			log.Println("[ERR] Shsz_detail.GrapWriter() : failed 2 open *tar.Writer :", sFilePath)
		}

	}

	return nil
}
