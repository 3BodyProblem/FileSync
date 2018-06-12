/**
 * @brief		File's Compressor Tools
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"path"
	"strconv"
	"strings"
)

// Package Initialization
func init() {
}

///////////////////////// dy column ///////////////////////////////////////////
type DYColumnRecordIO struct {
	BaseRecordIO
}

func (pSelf *DYColumnRecordIO) CodeInWhiteTable(sFileName string) bool {
	sTmpName := strings.ToLower(sFileName)

	if strings.Contains(sTmpName, "dybk.ini") == true {
		return true
	}

	return false
}

func (pSelf *DYColumnRecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 20120609, len(bytesData)
}

///////////////////////// gn column ///////////////////////////////////////////
type GNColumnRecordIO struct {
	BaseRecordIO
}

func (pSelf *GNColumnRecordIO) CodeInWhiteTable(sFileName string) bool {
	sTmpName := strings.ToLower(sFileName)

	if strings.Contains(sTmpName, "gnbk.ini") == true {
		return true
	}

	return false
}

func (pSelf *GNColumnRecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 20120609, len(bytesData)
}

///////////////////////// hy column ///////////////////////////////////////////
type HYColumnRecordIO struct {
	BaseRecordIO
}

func (pSelf *HYColumnRecordIO) CodeInWhiteTable(sFileName string) bool {
	sTmpName := strings.ToLower(sFileName)

	if strings.Contains(sTmpName, "hybk.ini") == true {
		return true
	}

	return false
}

func (pSelf *HYColumnRecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 20120609, len(bytesData)
}

///////////////////////// zs column ///////////////////////////////////////////
type ZSColumnRecordIO struct {
	BaseRecordIO
}

func (pSelf *ZSColumnRecordIO) CodeInWhiteTable(sFileName string) bool {
	sTmpName := strings.ToLower(sFileName)

	if strings.Contains(sTmpName, "zsbk.ini") == true {
		return true
	}

	return false
}

func (pSelf *ZSColumnRecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 20120609, len(bytesData)
}

///////////////////////// blockinfo.ini + nnn.ini //////////////////////////////////////
type BlkInfoRecordIO struct {
	BaseRecordIO
}

func (pSelf *BlkInfoRecordIO) CodeInWhiteTable(sFileName string) bool {
	_, sSplitFileName := path.Split(strings.ToLower(sFileName))

	if strings.Contains(sSplitFileName, "blockinfo.ini") == true && sSplitFileName[9] == '.' {
		return true
	}

	if len(sSplitFileName) > 5 {
		_, err := strconv.Atoi(sSplitFileName[:3])
		if err == nil && sSplitFileName[3] == '.' {
			return true
		}
	}

	return false
}

func (pSelf *BlkInfoRecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 20120609, len(bytesData)
}
