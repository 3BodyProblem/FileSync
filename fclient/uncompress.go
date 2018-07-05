/**
 * @brief		File's Uncompression Tools
 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
	"archive/tar"
	"compress/zlib"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

var (
	objCacheFileTable BufferFileTable // 全市场行情数据的落盘缓存[path,data]
)

// Package Initialization
func init() {
	objCacheFileTable.Initialize()
}

///////////////////////////////////// HTTP Client Engine Stucture/Class
type Uncompress struct {
	TargetFolder string // Root Folder
}

///////////////////////////////////// [OutterMethod]
// [method] Unzip
func (pSelf *Uncompress) Unzip(sZipSrcPath, sSubPath, sDataType string) bool {
	var err error
	var objBufFile I_BufferFile
	var sLastFilePath string = ""
	var sLocalFolder string = path.Dir(filepath.Join(pSelf.TargetFolder, sSubPath))
	var objMapFolder map[string]bool = make(map[string]bool, 1024*16)
	var sMkID string = strings.Split(sDataType, ".")[0]
	var sFileType string = strings.Split(sDataType, ".")[1]
	// open zip file
	if "windows" == runtime.GOOS {
		sLocalFolder = "./" + filepath.Join(pSelf.TargetFolder, sSubPath[:strings.LastIndex(sSubPath, "/")])
	}

	defer func(refBufFile I_BufferFile) {
		if refBufFile != nil {
			refBufFile.Close()
			refBufFile = nil
		}
	}(objBufFile)

	//////////// 对不同的文件类型，使用不同的写文件方式 ////////////////////////
	nFileOpenMode := os.O_RDWR | os.O_CREATE
	if false == strings.Contains(sSubPath, "HKSE") && false == strings.Contains(sSubPath, "QLFILE") && false == strings.Contains(sSubPath, "MIN1_TODAY") && false == strings.Contains(sSubPath, "STATIC.") && false == strings.Contains(sSubPath, "WEIGHT.") {
		nFileOpenMode |= os.O_APPEND
	} else {
		nFileOpenMode |= os.O_TRUNC
	}

	sZipSrcPath = strings.Replace(sZipSrcPath, "\\", "/", -1)
	sLocalFolder = strings.Replace(sLocalFolder, "\\", "/", -1)
	objZipReader, err := os.Open(sZipSrcPath)
	if err != nil {
		log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot open zip file :", sZipSrcPath, err.Error())
		return false
	}

	objGzipReader, err := zlib.NewReader(objZipReader)
	if err != nil {
		log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot open gzip reader :", sZipSrcPath, err.Error())
		return false
	}

	objTarReader := tar.NewReader(objGzipReader)
	defer objZipReader.Close()
	defer objGzipReader.Close()

	for {
		hdr, err := objTarReader.Next()
		if err == io.EOF {
			break // End of tar archive
		}

		if hdr.Typeflag != tar.TypeDir {
			sTargetFile := filepath.Join(sLocalFolder, hdr.Name)
			_, sSplitFileName := path.Split(sTargetFile)
			if strings.Contains(sSplitFileName, ".") == false {
				continue
			}
			//////////////////////////// 对Static文件，需要去掉路径和文件句中的日期信息
			nStaticIndex := strings.LastIndex(sTargetFile, "STATIC20")
			if nStaticIndex > 0 {
				nYear := time.Now().Year()
				nStaticIndex2 := strings.LastIndex(sTargetFile[:nStaticIndex], strconv.Itoa(nYear))
				if nStaticIndex2 > 0 {
					sTargetFile = sTargetFile[:nStaticIndex2] + "STATIC.csv"
				}
			}

			//////////////////////////// 预先创建好目录结构 /////////////////////////
			sTargetFolder := path.Dir(sTargetFile)
			if "windows" == runtime.GOOS {
				sTargetFolder = sTargetFile[:strings.LastIndex(sTargetFile, "\\")+1]
			}

			if _, ok := objMapFolder[sTargetFolder]; ok {
			} else {
				err = os.MkdirAll(sTargetFolder, 0755)
				if err != nil {
					log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot build target folder 4 tar file, folder: ", sTargetFolder, sLocalFolder, err.Error())
					return false
				}
				objMapFolder[sTargetFolder] = true
			}
			///////////////////////// 关闭旧文件/打开新文件 /////////////////////////
			if sLastFilePath != sTargetFile {
				objBufFile = objCacheFileTable.Open(sMkID, sFileType, sTargetFile, nFileOpenMode)
				if nil == objBufFile {
					return false
				}

				sLastFilePath = sTargetFile
			}
			///////////////////////// 写数据到文件 //////////////////////////////////
			if objBufFile != nil {
				if false == objBufFile.WriteFrom(objTarReader) {
					return false
				}
			}
		}
	}

	return true
}
