/**
 * @brief		File's Compressor Tools
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"archive/tar"
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

			if nDate < 19901010 || nDate > 20301010 { // Invalid Date
				continue
			}

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
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "WEIGHT/WEIGHT."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".d1") || (objDataSrc.MkID == "szse" && sDataType == ".d1"):
		objRecordIO := Day1RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of Day data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "DAY/DAY."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".m1") || (objDataSrc.MkID == "szse" && sDataType == ".m1"):
		objRecordIO := Minutes1RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of M1 data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "MIN/MIN."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".real_m1") || (objDataSrc.MkID == "szse" && sDataType == ".real_m1"):
		objRecordIO := RealMinutes1RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of M1 data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "MIN1_TODAY/MIN1_TODAY."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".m5") || (objDataSrc.MkID == "szse" && sDataType == ".m5"):
		objRecordIO := Minutes5RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of M5 data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "MIN5/MIN5."), objDataSrc.Folder, &objRecordIO)
	case (objDataSrc.MkID == "sse" && sDataType == ".m60") || (objDataSrc.MkID == "szse" && sDataType == ".m60"):
		objRecordIO := Minutes60RecordIO{BaseRecordIO: BaseRecordIO{CodeRangeFilter: codeRange, DataType: strings.ToLower(sResType)}} // policy of M60 data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "MIN60/MIN60."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".participant":
		objRecordIO := ParticipantRecordIO{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "Participant."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".shase_rzrq_by_date":
		objRecordIO := Shase_rzrq_by_date{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "shase_rzrq_by_date/shase_rzrq_by_date."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".sznse_rzrq_by_date":
		objRecordIO := Sznse_rzrq_by_date{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "sznse_rzrq_by_date/sznse_rzrq_by_date."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".shsz_idx_by_date":
		objRecordIO := Shsz_idx_by_date{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "shsz_idx_by_date/shsz_idx_by_date."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "hkse" && sDataType == ".shsz_detail":
		objRecordIO := Shsz_detail{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "shsz_detail/shsz_detail."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "qlfile" && sDataType == ".column_dy_bk":
		objRecordIO := DYColumnRecordIO{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "dybk."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "qlfile" && sDataType == ".column_gn_bk":
		objRecordIO := GNColumnRecordIO{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "gnbk."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "qlfile" && sDataType == ".column_hy_bk":
		objRecordIO := HYColumnRecordIO{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "hybk."), objDataSrc.Folder, &objRecordIO)
	case objDataSrc.MkID == "qlfile" && sDataType == ".column_zs_bk":
		objRecordIO := ZSColumnRecordIO{BaseRecordIO: BaseRecordIO{DataType: strings.ToLower(sResType)}} // policy of hk data loader
		return pSelf.TranslateFolder(filepath.Join(sDestFolder, "zsbk."), objDataSrc.Folder, &objRecordIO)
	default:
		log.Printf("[ERR] Compressor.XCompress() : [Compressing] invalid exchange code(%s) or data type(%s)", objDataSrc.MkID, sDataType)
		return lstRes, false
	}
}

///////////////////////////////////// [InnerMethod] ///////////////////////////////////////////
// [Method] load source data 2 targer folder
func (pSelf *Compressor) TranslateFolder(sDestFile, sSrcFolder string, pILoader I_Record_IO) ([]ResDownload, bool) {
	var lstRes []ResDownload
	var sMkFolder string = path.Dir(sDestFile)
	//////////////// Prepare Data Folder && File Handles
	if "windows" == runtime.GOOS {
		sMkFolder = sDestFile[:strings.LastIndex(sDestFile, "\\")]
	}
	sDestFile = strings.Replace(sDestFile, "\\", "/", -1)

	err := os.MkdirAll(sMkFolder, 0755)
	if err != nil {
		log.Println("[ERR] Compressor.TranslateFolder() : cannot build target folder 4 zip file :", sMkFolder)
		return lstRes, false
	}
	///////////////// Initialize Object type(I_Record_IO)
	log.Printf("[INF] Compressor.TranslateFolder() : compressing ---> (%s)", sSrcFolder)
	if false == pILoader.Initialize() {
		log.Println("[ERR] Compressor.TranslateFolder() : Cannot initialize I_Record_IO object, ", sSrcFolder)
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
