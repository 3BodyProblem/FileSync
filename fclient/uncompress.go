/**
 * @brief		File's Comparison Tools
 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
	"archive/tar"
	"compress/gzip"
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
	// open zip file
	sLocalFolder := path.Dir(filepath.Join(pSelf.TargetFolder, sSubPath))
	if "windows" == runtime.GOOS {
		sLocalFolder = "./" + filepath.Join(pSelf.TargetFolder, sSubPath[:strings.LastIndex(sSubPath, "/")])
	}

	sZipSrcPath = strings.Replace(sZipSrcPath, "\\", "/", -1)
	sLocalFolder = strings.Replace(sLocalFolder, "\\", "/", -1)
	log.Printf("[INF] Uncompress.Unzip() : [Uncompressing] zip file %s --> %s", sZipSrcPath, sLocalFolder)
	objZipReader, err := os.Open(sZipSrcPath)
	if err != nil {
		log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot open zip file :", sZipSrcPath, err.Error())
		return false
	}

	objGzipReader, err := gzip.NewReader(objZipReader)
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
			// Create diretory before create file
			sTargetFile := filepath.Join(sLocalFolder, hdr.Name)
			_, sSplitFileName := path.Split(sTargetFile)
			if strings.Contains(sSplitFileName, ".") == false {
				continue
			}
			sTargetFolder := path.Dir(sTargetFile)
			if "windows" == runtime.GOOS {
				sTargetFolder = sTargetFile[:strings.LastIndex(sTargetFile, "\\")+1]
			}
			err = os.MkdirAll(sTargetFolder, 0755)
			if err != nil {
				log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot build target folder 4 tar file, folder: ", sTargetFolder, err.Error())
				return false
			}

			// Write data to file
			sTargetFile = strings.Replace(sTargetFile, "\\", "/", -1)
			fw, err := os.OpenFile(sTargetFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot create tar file, file name =", sTargetFile, err.Error())
				return false
			}
			defer fw.Close()

			nFileSize, _ := fw.Seek(0, os.SEEK_END)
			if strings.LastIndex(sTargetFile, "/MIN/") > 0 && nFileSize == 0 {
				fw.WriteString("date,time,openpx,highpx,lowpx,closepx,settlepx,amount,volume,openinterest,numtrades,voip\n")
			}
			if strings.LastIndex(sTargetFile, "/MIN5/") > 0 && nFileSize == 0 {
				fw.WriteString("date,time,openpx,highpx,lowpx,closepx,settlepx,amount,volume,openinterest,numtrades,voip\n")
			}
			if strings.LastIndex(sTargetFile, "/DAY/") > 0 && nFileSize == 0 {
				fw.WriteString("date,openpx,highpx,lowpx,closepx,settlepx,amount,volume,openinterest,numtrades,voip\n")
			}

			_, err = io.Copy(fw, objTarReader)
			if err != nil {
				log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot write tar file, file name =", sTargetFile, err.Error())
				fw.Close()
				return false
			}
			fw.Close()
		}
	}

	return true
}

///////////////////////////////////// [InnerMethod]
