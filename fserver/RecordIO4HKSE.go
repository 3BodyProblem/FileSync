/**
 * @brief		File's Compressor Tools
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"archive/tar"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

// Package Initialization
func init() {
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
	return bytesData, 20120609, len(bytesData)
}

///////////////////////// shase_rzrq_by_date Lines ///////////////////////////////////////////
type Shase_rzrq_by_date struct {
	BaseRecordIO
}

func (pSelf *Shase_rzrq_by_date) CodeInWhiteTable(sFileName string) bool {
	return true
}

func (pSelf *Shase_rzrq_by_date) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 20120609, len(bytesData)
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
		if nDate/10000 < objToday.Year() { // Not Current Year
			sFile = fmt.Sprintf("%s%d", sFilePath, nDate/10000*10000)
		} else { // Is Current Year
			nDD := (nDate % 100) ////////// One File With 2 Week's Data Inside
			if nDD <= 15 {
				nDD = 0
			} else {
				nDD = 15
			}
			sFile = fmt.Sprintf("%s%d", sFilePath, nDate/100*100+nDD) // 如果不是近期，则目标压缩文件，半个月的数据一个文件名(带上下月信息)
		}
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
	return bytesData, 20120609, len(bytesData)
}

func (pSelf *Sznse_rzrq_by_date) GrapWriter(sFilePath string, nDate int, sSrcFile string) *tar.Writer {
	var err error
	var sFile string = ""
	var objToday time.Time = time.Now()

	lstPath := strings.Split(sSrcFile, "/")
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
		if nDate/10000 < objToday.Year() { // Not Current Year
			sFile = fmt.Sprintf("%s%d", sFilePath, nDate/10000*10000)
		} else { // Is Current Year
			nDD := (nDate % 100) ////////// One File With 2 Week's Data Inside
			if nDD <= 15 {
				nDD = 0
			} else {
				nDD = 15
			}
			sFile = fmt.Sprintf("%s%d", sFilePath, nDate/100*100+nDD) // 如果不是近期，则目标压缩文件，半个月的数据一个文件名(带上下月信息)
		}
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
	return bytesData, 20120609, len(bytesData)
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
		if nDate/10000 < objToday.Year() { // Not Current Year
			sFile = fmt.Sprintf("%s%d", sFilePath, nDate/10000*10000)
		} else { // Is Current Year
			nDD := (nDate % 100) ////////// One File With 2 Week's Data Inside
			if nDD <= 15 {
				nDD = 0
			} else {
				nDD = 15
			}
			sFile = fmt.Sprintf("%s%d", sFilePath, nDate/100*100+nDD) // 如果不是近期，则目标压缩文件，半个月的数据一个文件名(带上下月信息)
		}
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
	return bytesData, 20120609, len(bytesData)
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
		if nDate/10000 < objToday.Year() { // Not Current Year
			sFile = fmt.Sprintf("%s%d", sFilePath, nDate/10000*10000)
		} else { // Is Current Year
			nDD := (nDate % 100) ////////// One File With 2 Week's Data Inside
			if nDD <= 15 {
				nDD = 0
			} else {
				nDD = 15
			}
			sFile = fmt.Sprintf("%s%d", sFilePath, nDate/100*100+nDD) // 如果不是近期，则目标压缩文件，半个月的数据一个文件名(带上下月信息)
		}
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
