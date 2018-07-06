/**
 * @brief		带缓存的文件类
 * @author		barry
 * @date		2018/4/10
 */
package fclient

import (
	"archive/tar"
	"bytes"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
)

////////////////////////// 缓存文件类 ///////////////////////////////
/**
 * @Class 		I_BufferFile
 * @brief		缓存写盘文件管理接口
 * @author		barry
 */
type I_BufferFile interface {
	/**
	 * @brief			打开/创建文件
	 * @param[in]		sFilePath		文件路径
	 * @param[in]		nFileOpenMode	写文件权限
	 * @return			操作是否成功, true :成功
	 */
	Open(sFilePath string, nFileOpenMode int) bool

	/**
	 * @brief			关闭文件
	 * @author			barry
	 */
	Close()

	/**
	 * @brief			写数据到缓存文件
	 * @param[in]		sMkID			市场编号
	 * @param[in]		sFileType		文件类型
	 * @author			barry
	 */
	WriteFrom(pTarFile *tar.Reader) bool

	/**
	 * @brief			刷数据到文件
	 * @param[in]		sFilePath		目标文件路径
	 * @return			true			成功
	 * @author			barry
	 */
	Flush2File(sFilePath string) bool
}

/**
 * @class			BufferFile
 * @brief			缓存文件类
 * @author			barry
 */
type BufferFile struct {
	MkID       string       // 市场编号 sse/szse
	DataType   string       // 文件类型 .d1/.m1
	FilePtr    *os.File     // 文件对象指针
	FileBuffer bytes.Buffer // 文件数据缓存
}

///< ---------------------- [Public 方法] -----------------------------
/**
 * @brief			打开/创建文件
 * @param[in]		sFilePath		文件路径
 * @param[in]		nFileOpenMode	写文件权限
 * @return			操作是否成功, true :成功
 */
func (pSelf *BufferFile) Open(sFilePath string, nFileOpenMode int) bool {
	pSelf.Close() // 先关闭文件
	if nil == pSelf.FilePtr {
		var err error

		pSelf.FilePtr, err = os.OpenFile(sFilePath, nFileOpenMode, 0644)
		if err != nil {
			log.Println("[ERR] BufferFile.Open() : cannot create/open file, path =", sFilePath, err.Error())
			return false
		}
		///////////////////////// 如果是创建的新文件，先写入title ///////////////////
		objStatus, _ := pSelf.FilePtr.Stat()
		if objStatus.Size() < 10 {
			sFilePath = strings.Replace(sFilePath, "\\", "/", -1)
			if strings.LastIndex(sFilePath, "/MIN/") > 0 || strings.LastIndex(sFilePath, "/MIN1_TODAY/") > 0 {
				pSelf.FilePtr.WriteString("date,time,openpx,highpx,lowpx,closepx,settlepx,amount,volume,openinterest,numtrades,voip\n")
			}
			if strings.LastIndex(sFilePath, "/MIN5/") > 0 {
				pSelf.FilePtr.WriteString("date,time,openpx,highpx,lowpx,closepx,settlepx,amount,volume,openinterest,numtrades,voip\n")
			}
			if strings.LastIndex(sFilePath, "/MIN60/") > 0 {
				pSelf.FilePtr.WriteString("date,time,openpx,highpx,lowpx,closepx,settlepx,amount,volume,openinterest,numtrades,voip\n")
			}
			if strings.LastIndex(sFilePath, "/DAY/") > 0 {
				pSelf.FilePtr.WriteString("date,openpx,highpx,lowpx,closepx,settlepx,amount,volume,openinterest,numtrades,voip\n")
			}
			if strings.LastIndex(sFilePath, "/STATIC/") > 0 {
				pSelf.FilePtr.WriteString("code,name,lotsize,contractmult,contractunit,startdate,enddate,xqdate,deliverydate,expiredate,underlyingcode,underlyingname,optiontype,callorput,exercisepx\n")
			}
		}
	}

	return true
}

/**
 * @brief			关闭文件
 * @author			barry
 */
func (pSelf *BufferFile) Close() {
	if nil != pSelf.FilePtr {
		pSelf.FilePtr.Close()
		pSelf.FilePtr = nil
	}
}

/**
 * @brief			写数据到缓存文件
 * @param[in]		sMkID			市场编号
 * @param[in]		sFileType		文件类型
 * @author			barry
 */
func (pSelf *BufferFile) WriteFrom(pTarFile *tar.Reader) bool {
	var err error

	if "d1" == pSelf.DataType {
		/////////// 日线数据需要缓存，缓存到达一定直后再写盘 ///////////
		_, err = io.Copy(&(pSelf.FileBuffer), pTarFile)
		if err != nil {
			log.Println("[ERR] BufferFile.Write() : cannot write to buffer, MkID & Type =", pSelf.MkID, pSelf.DataType, err.Error(), pSelf.FilePtr)
		}

		if pSelf.FileBuffer.Len() > 1024*25 { // 设置： 日线缓存过25k时才写盘
			pSelf.FilePtr.Write(pSelf.FileBuffer.Bytes()) // 将缓存中的数据全部写文件
			pSelf.FileBuffer.Reset()                      // 写完缓存中的数据后，清空缓存
		}
	} else {
		/////////// 非日线数据, 直接写盘 /////////////////////////////
		_, err = io.Copy(pSelf.FilePtr, pTarFile)
		if err != nil {
			log.Println("[ERR] BufferFile.Write() : cannot write tar file, MkID & Type =", pSelf.MkID, pSelf.DataType, err.Error(), pSelf.FilePtr)
			if pSelf.FilePtr != nil {
				pSelf.FilePtr.Close()
			}

			return false
		}
	}

	return true
}

/**
 * @brief			刷数据到文件
 * @param[in]		sFilePath		目标文件路径
 * @return			true			成功
 * @author			barry
 */
func (pSelf *BufferFile) Flush2File(sFilePath string) bool {
	if 0 == pSelf.FileBuffer.Len() {
		return true
	}

	pFilePtr, err := os.OpenFile(sFilePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Println("[ERR] BufferFile.Flush2File() : cannot flush (%s.%s)file, path=%s, error=%s", pSelf.MkID, pSelf.DataType, sFilePath, err.Error())
		return false
	}

	defer pFilePtr.Close()
	pFilePtr.Write(pSelf.FileBuffer.Bytes())
	pSelf.FileBuffer.Reset()

	return true
}

//////////////////////// 缓存文件哈希表 //////////////////////
/**
 * @class			BufferFileTable
 * @brief			缓存文件哈希表类
 * @author			barry
 */
type BufferFileTable struct {
	objLock           *sync.Mutex             // 资源清单锁
	objCacheFileTable map[string]I_BufferFile // 全市场行情数据的落盘缓存[path,data]
	objMapFolder      map[string]bool         // 创建过的子目录表
}

/**
 * @brief			初始化
 */
func (pSelf *BufferFileTable) Initialize() bool {
	pSelf.objLock = new(sync.Mutex)
	pSelf.objCacheFileTable = make(map[string]I_BufferFile)
	pSelf.objMapFolder = make(map[string]bool, 1024*16)

	return true
}

/**
 * @brief			打开/创建文件
 * @param[in]		sMkID			市场编号
 * @param[in]		sFileType		文件数据类型
 * @param[in]		sFilePath		文件路径
 * @param[in]		nFileOpenMode	写文件权限
 * @return			操作是否成功, true :成功
 */
func (pSelf *BufferFileTable) Open(sMkID, sFileType, sFilePath string, nFileOpenMode int) I_BufferFile {
	pSelf.objLock.Lock()
	defer pSelf.objLock.Unlock()
	//////////////////////////// 预先创建好目录结构 /////////////////////////
	sTargetFolder := path.Dir(sFilePath)
	if "windows" == runtime.GOOS {
		sTargetFolder = sFilePath[:strings.LastIndex(sFilePath, "\\")+1]
	}

	if _, ok := pSelf.objMapFolder[sTargetFolder]; false == ok {
		err := os.MkdirAll(sTargetFolder, 0644)
		if err != nil {
			log.Println("[ERR] BufferFileTable.Open() : cannot build target folder: ", sTargetFolder, err.Error())
			return nil
		}
		pSelf.objMapFolder[sTargetFolder] = true
	}
	/////////////////////////// 打开文件 //////////////////////////////////
	if _, ok := pSelf.objCacheFileTable[sFilePath]; false == ok {
		objNewBuffFile := BufferFile{MkID: sMkID, DataType: sFileType, FilePtr: nil}
		pSelf.objCacheFileTable[sFilePath] = &objNewBuffFile // 创建未打开过的缓存文件并设置到Map
	}

	objBufFile := pSelf.objCacheFileTable[sFilePath]
	if false == objBufFile.Open(sFilePath, nFileOpenMode) {
		log.Println("[ERR] BufferFileTable.Open() : cannot open/create file: ", sFilePath)
		return nil
	}

	return objBufFile
}

/**
 * @brief			刷缓存中的最后数据到文件
 * @return			true			成功
 * @author			barry
 */
func (pSelf *BufferFileTable) FlushBuffer2File() {
	pSelf.objLock.Lock()
	defer pSelf.objLock.Unlock()

	if len(pSelf.objCacheFileTable) == 0 {
		return
	}

	log.Println("[INF] BufferFileTable.FlushBuffer2File() : [Flushing] ...........")

	for sFilePath, objCacheFile := range pSelf.objCacheFileTable {
		objCacheFile.Flush2File(sFilePath)
	}

	log.Println("[INF] BufferFileTable.FlushBuffer2File() : [DONE] all data in buffer flushed 2 disk...")
}
