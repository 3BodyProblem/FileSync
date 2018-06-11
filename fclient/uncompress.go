/**
 * @brief		File's Comparison Tools
 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
	"archive/tar"
	//"compress/gzip"
	"compress/zlib"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
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
	var err error
	var pTarFile *os.File = nil
	var sLastFilePath string = ""
	var sLocalFolder string = path.Dir(filepath.Join(pSelf.TargetFolder, sSubPath))
	var objMapFolder map[string]bool = make(map[string]bool, 1024*16)
	// open zip file
	if "windows" == runtime.GOOS {
		sLocalFolder = "./" + filepath.Join(pSelf.TargetFolder, sSubPath[:strings.LastIndex(sSubPath, "/")])
	}

	nFileOpenMode := os.O_RDWR | os.O_CREATE
	if false == strings.Contains(sSubPath, "HKSE") && false == strings.Contains(sSubPath, "QLFILE") {
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

	objGzipReader, err := zlib.NewReader(objZipReader) // gzip.NewReader(objZipReader)
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
		// Check if it is diretory or file
		if hdr.Typeflag != tar.TypeDir {
			// Get files from archive
			//////////////////////////// Create Folder ////////////////////////////////
			sTargetFile := filepath.Join(sLocalFolder, hdr.Name)
			_, sSplitFileName := path.Split(sTargetFile)
			if strings.Contains(sSplitFileName, ".") == false {
				continue
			}
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

			///////////////////////// Open File ///////////////////////////////////////
			if sLastFilePath != sTargetFile {
				if pTarFile != nil {
					pTarFile.Close()
					pTarFile = nil
				}

				pTarFile, err = os.OpenFile(sTargetFile, nFileOpenMode, 0644)
				if err != nil {
					log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot create tar file, file name =", sTargetFile, sLocalFolder, err.Error())
					return false
				}
				sLastFilePath = sTargetFile
				objStatus, _ := pTarFile.Stat()
				///////////////////////// Write data to file ///////////////////////////////
				if objStatus.Size() < 10 {
					/////////////////// Check Title In File ///////////////////////
					sTargetFile = strings.Replace(sTargetFile, "\\", "/", -1)
					if strings.LastIndex(sTargetFile, "/MIN/") > 0 || strings.LastIndex(sTargetFile, "/MIN1_TODAY/") > 0 {
						pTarFile.WriteString("date,time,openpx,highpx,lowpx,closepx,settlepx,amount,volume,openinterest,numtrades,voip\n")
					}
					if strings.LastIndex(sTargetFile, "/MIN5/") > 0 {
						pTarFile.WriteString("date,time,openpx,highpx,lowpx,closepx,settlepx,amount,volume,openinterest,numtrades,voip\n")
					}
					if strings.LastIndex(sTargetFile, "/MIN60/") > 0 {
						pTarFile.WriteString("date,time,openpx,highpx,lowpx,closepx,settlepx,amount,volume,openinterest,numtrades,voip\n")
					}
					if strings.LastIndex(sTargetFile, "/DAY/") > 0 {
						pTarFile.WriteString("date,openpx,highpx,lowpx,closepx,settlepx,amount,volume,openinterest,numtrades,voip\n")
					}
				}
			}

			_, err = io.Copy(pTarFile, objTarReader)
			if err != nil {
				log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot write tar file, file name =", sTargetFile, sLocalFolder, err.Error())
				if pTarFile != nil {
					pTarFile.Close()
				}
				return false
			}
		}
	}

	if pTarFile != nil {
		pTarFile.Close()
	}

	return true
}

///////////////////////////////////// [InnerMethod]
