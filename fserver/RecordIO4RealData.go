/**
 * @brief		File's Compressor Tools
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"bytes"
	"log"
	"strconv"
	"strings"
	"time"
)

// Package Initialization
func init() {
}

///////////////////////// 1Minutes Lines ///////////////////////////////////////////
type RealMinutes1RecordIO struct {
	BaseRecordIO
}

func (pSelf *RealMinutes1RecordIO) CodeInWhiteTable(sFileName string) bool {
	if pSelf.CodeRangeFilter == nil {
		return true
	}

	nEnd := strings.LastIndexAny(sFileName, ".")
	nFileYear, err := strconv.Atoi(sFileName[nEnd-4 : nEnd])
	if nil != err {
		log.Println("[ERR] RealMinutes1RecordIO.CodeInWhiteTable() : Year In FileName is not digital: ", sFileName, nFileYear)
		return false
	}
	if time.Now().Year() != nFileYear {
		return false
	}
	nBegin := strings.LastIndexAny(sFileName, "MIN")
	nEnd = nEnd - 5
	sCodeNum := sFileName[nBegin+1 : nEnd]

	return pSelf.CodeRangeFilter.CodeInRange(sCodeNum)
}

func (pSelf *RealMinutes1RecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	var rstr string = ""
	var objToday time.Time = time.Now()
	var nToday int = objToday.Year()*10000 + int(objToday.Month())*100 + objToday.Day()

	lstRecords := bytes.Split(bytesData, []byte("\n"))
	nListLen := len(lstRecords)
	for n := nListLen - 1; n >= 0; n-- {
		sLine := string(lstRecords[n])
		sFirstFields := strings.Split(sLine, ",")[0]
		if len(sFirstFields) <= 0 {
			continue
		}
		nDate, err := strconv.Atoi(sFirstFields)
		if err != nil {
			continue
		}

		if nToday != nDate {
			break
		}

		rstr = (sLine + "\n") + rstr
	}

	return []byte(rstr), nToday, len(bytesData)
}
