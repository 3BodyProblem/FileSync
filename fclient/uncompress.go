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
		sLocalFolder = "./" + filepath.Join(pSelf.TargetFolder, sSubPath[:strings.LastIndex(sSubPath, "\\")])
	}

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
				sTargetFolder = sTargetFile[:strings.LastIndex(sTargetFile, "\\")]
			}
			err = os.MkdirAll(sTargetFolder+"/", 0755)
			if err != nil {
				log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot build target folder 4 tar file, file name =", sTargetFile)
				return false
			}

			// Write data to file
			fw, err := os.Create(sTargetFile)
			if err != nil {
				log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot create tar file, file name =", sTargetFile)
				return false
			}

			defer fw.Close()
			_, err = io.Copy(fw, objTarReader)
			if err != nil {
				log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot write tar file, file name =", sTargetFile)
				return false
			}
		}
	}

	/*


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
			if "windows" == runtime.GOOS {
				sTargetFolder = sTargetFile[:strings.LastIndex(sTargetFile, "\\")]
			}
			err = os.MkdirAll(sTargetFolder, 0755)
			if err != nil {
				log.Println("[ERR] Uncompress.Unzip() : [Uncompressing] cannot build target folder 4 zip file, file name =", sTargetFile)
				return false
			}

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
		}
	*/
	return true
}

///////////////////////////////////// [InnerMethod]
