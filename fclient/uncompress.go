/**
 * @brief		行情资源包解压、分派子目录转存类
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

func init() {
	objCacheFileTable.Initialize()
}

////////////////////////// 解压类 /////////////////////////////////////
type Uncompress struct {
	TargetFolder string // 解压数据文件存放的根目录
}

///< ---------------------- [Public 方法] -----------------------------
/**
* @brief			解压函数
* @param[in]		sZipSrcPath			资源压缩包路径
* @param[in]		sSubPath			资源所在URI
* @param[in]		sDataType			资源类型
* @return			true				解压成功
					false				解压失败
*/
func (pSelf *Uncompress) Unzip(sZipSrcPath, sSubPath, sDataType string) bool {
	var err error                                                                   // 错误信号
	var objBufFile I_BufferFile                                                     // 输出文件句柄接口
	var sLastFilePath string = ""                                                   // 最后一次写入文件的路径，用来作为判断是否要打开新文件的依据
	var sLocalFolder string = path.Dir(filepath.Join(pSelf.TargetFolder, sSubPath)) // 输出文件目录

	if "windows" == runtime.GOOS {
		sLocalFolder = "./" + filepath.Join(pSelf.TargetFolder, sSubPath[:strings.LastIndex(sSubPath, "/")])
	}
	/////////// 确保文件关闭
	defer func(refBufFile I_BufferFile) {
		if refBufFile != nil {
			refBufFile.Close()
		}
	}(objBufFile)
	//////////// 对不同的文件类型，使用不同的写文件方式 ////////////////////////
	nFileOpenMode := os.O_RDWR | os.O_CREATE
	if false == strings.Contains(sSubPath, "HKSE") && false == strings.Contains(sSubPath, "QLFILE") && false == strings.Contains(sSubPath, "MIN1_TODAY") && false == strings.Contains(sSubPath, "STATIC.") && false == strings.Contains(sSubPath, "WEIGHT.") {
		nFileOpenMode |= os.O_APPEND
	} else {
		nFileOpenMode |= os.O_TRUNC
	}
	//////////// 打开资源压缩包 /////////////////////////////////////////////
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
	/////////// 遍历资源压缩包，把数据分别转存到对应的子目录、子文件 ////////////////////
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
			///////////////////////// 关闭旧文件/打开新文件 /////////////////////////
			if sLastFilePath != sTargetFile {
				if objBufFile != nil {
					objBufFile.Close()
					objBufFile = nil
				}

				var sMkID string = strings.Split(sDataType, ".")[0]     // 市场编号
				var sFileType string = strings.Split(sDataType, ".")[1] // 文件数据类型
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
