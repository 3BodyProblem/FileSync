/**
 * @brief		File's Compressor Tools
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"archive/tar"
	"bytes"
	"compress/zlib"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

// Package Initialization
func init() {
}

///////////////////////// 60Minutes Lines ///////////////////////////////////////////
type Minutes60RecordIO struct {
	BaseRecordIO
}

func (pSelf *Minutes60RecordIO) CodeInWhiteTable(sFileName string) bool {
	if pSelf.CodeRangeFilter == nil {
		return true
	}

	nEnd := strings.LastIndexAny(sFileName, ".")
	nFileYear, err := strconv.Atoi(sFileName[nEnd-4 : nEnd])
	if nil != err {
		log.Println("[ERR] Minutes1RecordIO.CodeInWhiteTable() : Year In FileName is not digital: ", sFileName, nFileYear)
		return false
	}
	if time.Now().Year()-nFileYear >= 4 {
		return false
	}
	nBegin := strings.LastIndexAny(sFileName, "MIN")
	nEnd = nEnd - 5
	sCodeNum := sFileName[nBegin+1 : nEnd]

	return pSelf.CodeRangeFilter.CodeInRange(sCodeNum)
}

func (pSelf *Minutes60RecordIO) GenFilePath(sFileName string) string {
	return strings.Replace(sFileName, "MIN/", "MIN60/", -1)
}

func (pSelf *Minutes60RecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	var err error
	var nLastDate int = 0
	var nOffset int = 0
	var nReturnDate int = -100
	var objToday time.Time = time.Now()
	var nToday int = objToday.Year()*10000 + int(objToday.Month())*100 + objToday.Day()
	var nNowTime int = objToday.Hour()*10000 + objToday.Minute()*100 + objToday.Second()
	var bLoadTodayData bool = false
	var rstr string = ""
	var lstPeriods = [4]int{103000, 130000, 140000, 150000}
	var nLastIndex int = -1
	var nCurIndex int = 0
	var objMin60 struct {
		Date         int     // date
		Time         int     // time
		Open         float64 // open price
		High         float64 // high price
		Low          float64 // low price
		Close        float64 // close price
		Settle       float64 // settle price
		Amount       float64 // Amount
		Volume       int64   // Volume
		OpenInterest int64   // Open Interest
		NumTrades    int64   // Trade Number
		Voip         float64 // Voip
	} // 60 minutes k-line

	bNewBegin := true
	nLastOffset := 0
	bSep := byte('\n')
	nBytesLen := len(bytesData)
	if nNowTime < 70101 || nNowTime >= 220010 { // exclude current data of today in working time
		bLoadTodayData = true
	}

	for nOffset = 0; nOffset < nBytesLen; nOffset++ {
		if bytesData[nOffset] != bSep {
			continue
		}

		lstRecords := strings.Split(string(bytesData[nLastOffset:nOffset]), ",")
		nLastOffset = nOffset + 1
		if len(lstRecords[0]) <= 0 {
			continue
		}

		objMin60.Date, err = strconv.Atoi(lstRecords[0])
		if err != nil || 20180618 == objMin60.Date {
			continue
		}

		objRecordDate := time.Date(objMin60.Date/10000, time.Month(objMin60.Date%10000/100), objMin60.Date%100, 21, 6, 9, 0, time.Local)
		subHours := objToday.Sub(objRecordDate)
		nDays := subHours.Hours() / 24
		if nDays > 366*3 {
			continue
		}

		if nToday == objMin60.Date && false == bLoadTodayData {
			bNewBegin = false
			continue
		}

		if -100 == nReturnDate {
			nReturnDate = objMin60.Date
		}

		// cal. 60 minutes k-lines
		nCurTime, _ := strconv.Atoi(lstRecords[1])
		objMin60.Close, _ = strconv.ParseFloat(lstRecords[5], 64)
		objMin60.Settle, _ = strconv.ParseFloat(lstRecords[6], 64)
		objMin60.Voip, _ = strconv.ParseFloat(lstRecords[11], 64)

		nPeriodTime := 0
		if nCurTime >= 63000 && nCurTime < 103000 {
			nCurIndex = 0
			nPeriodTime = 103000
		} else if nCurTime >= 103000 && nCurTime <= 113000 {
			nCurIndex = 1
			nPeriodTime = 113000
		} else if nCurTime > 113000 && nCurTime < 140000 {
			nCurIndex = 2
			nPeriodTime = 140000
		} else if nCurTime >= 140000 && nCurTime <= 160000 {
			nCurIndex = 3
			nPeriodTime = 150000
		} else {
			continue
		}

		if nReturnDate != objMin60.Date {
			bNewBegin = false
			rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", nLastDate, objMin60.Time, objMin60.Open, objMin60.High, objMin60.Low, objMin60.Close, objMin60.Settle, objMin60.Amount, objMin60.Volume, objMin60.OpenInterest, objMin60.NumTrades, objMin60.Voip)
			return []byte(rstr), nReturnDate, nOffset
		}

		nLastDate = objMin60.Date
		if nLastIndex != nCurIndex {
			nLastIndex = nCurIndex

			if 0 == objMin60.Time {
				objMin60.Time = lstPeriods[0]
				if nCurIndex > 0 {
					objMin60.Time = lstPeriods[nCurIndex-1]
				}
			}

			if nCurIndex > 0 {
				bNewBegin = false
				rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", objMin60.Date, objMin60.Time, objMin60.Open, objMin60.High, objMin60.Low, objMin60.Close, objMin60.Settle, objMin60.Amount, objMin60.Volume, objMin60.OpenInterest, objMin60.NumTrades, objMin60.Voip)
			}

			bNewBegin = true
			objMin60.Time = nPeriodTime
			objMin60.Open = objMin60.Close
			objMin60.High = objMin60.Close
			objMin60.Low = objMin60.Close
			objMin60.Amount, _ = strconv.ParseFloat(lstRecords[7], 64)
			objMin60.Volume, _ = strconv.ParseInt(lstRecords[8], 10, 64)
			objMin60.OpenInterest, _ = strconv.ParseInt(lstRecords[9], 10, 64)
			objMin60.NumTrades, _ = strconv.ParseInt(lstRecords[10], 10, 64)
		} else {
			objMin60.Time = nPeriodTime
			if objMin60.Close > objMin60.High {
				objMin60.High = objMin60.Close
			}

			if objMin60.Close < objMin60.Low {
				objMin60.Low = objMin60.Close
			}

			nAmount, _ := strconv.ParseFloat(lstRecords[7], 64)
			objMin60.Amount += nAmount
			nVolume, _ := strconv.ParseInt(lstRecords[8], 10, 64)
			objMin60.Volume += nVolume
			nOpenInterest, _ := strconv.ParseInt(lstRecords[9], 10, 64)
			objMin60.OpenInterest += nOpenInterest
			nNumTrades, _ := strconv.ParseInt(lstRecords[10], 10, 64)
			objMin60.NumTrades += nNumTrades
		}
	}

	if true == bNewBegin {
		if objMin60.Time > 0 {
			rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", objMin60.Date, objMin60.Time, objMin60.Open, objMin60.High, objMin60.Low, objMin60.Close, objMin60.Settle, objMin60.Amount, objMin60.Volume, objMin60.OpenInterest, objMin60.NumTrades, objMin60.Voip)
		}
	}

	return []byte(rstr), nReturnDate, len(bytesData)
}

///////////////////////// 5Minutes Lines ///////////////////////////////////////////
type Minutes5RecordIO struct {
	BaseRecordIO
}

func (pSelf *Minutes5RecordIO) CodeInWhiteTable(sFileName string) bool {
	if pSelf.CodeRangeFilter == nil {
		return true
	}

	nEnd := strings.LastIndexAny(sFileName, ".")
	nFileYear, err := strconv.Atoi(sFileName[nEnd-4 : nEnd])
	if nil != err {
		log.Println("[ERR] Minutes1RecordIO.CodeInWhiteTable() : Year In FileName is not digital: ", sFileName, nFileYear)
		return false
	}
	if time.Now().Year()-nFileYear >= 2 {
		return false
	}
	nBegin := strings.LastIndexAny(sFileName, "MIN")
	nEnd = nEnd - 5
	sCodeNum := sFileName[nBegin+1 : nEnd]

	return pSelf.CodeRangeFilter.CodeInRange(sCodeNum)
}

func (pSelf *Minutes5RecordIO) GenFilePath(sFileName string) string {
	return strings.Replace(sFileName, "MIN/", "MIN5/", -1)
}

func (pSelf *Minutes5RecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	var err error
	var nOffset int = 0
	var bLine []byte
	var i int = 0
	var nReturnDate int = -100
	var objToday time.Time = time.Now()
	var rstr string = ""
	var objMin5 struct {
		Date         int     // date
		Time         int     // time
		Open         float64 // open price
		High         float64 // high price
		Low          float64 // low price
		Close        float64 // close price
		Settle       float64 // settle price
		Amount       float64 // Amount
		Volume       int64   // Volume
		OpenInterest int64   // Open Interest
		NumTrades    int64   // Trade Number
		Voip         float64 // Voip
	} // 5 minutes k-line

	bLines := bytes.Split(bytesData, []byte("\n"))
	nCount := len(bLines)
	for i, bLine = range bLines {
		nOffset += (len(bLine) + 1)
		lstRecords := strings.Split(string(bLine), ",")
		if len(lstRecords[0]) <= 0 {
			continue
		}
		objMin5.Date, err = strconv.Atoi(lstRecords[0])
		if err != nil {
			continue
		}

		objRecordDate := time.Date(objMin5.Date/10000, time.Month(objMin5.Date%10000/100), objMin5.Date%100, 21, 6, 9, 0, time.Local)
		subHours := objToday.Sub(objRecordDate)
		nDays := subHours.Hours() / 24
		if nDays > 366 {
			continue
		}

		if -100 == nReturnDate {
			nReturnDate = objMin5.Date
		}

		if nReturnDate != objMin5.Date {
			return []byte(rstr), nReturnDate, nOffset
		}

		// cal. 5 minutes k-lines
		nCurTime, _ := strconv.Atoi(lstRecords[1])
		nCurTime /= 100000
		objMin5.Close, _ = strconv.ParseFloat(lstRecords[5], 64)
		objMin5.Settle, _ = strconv.ParseFloat(lstRecords[6], 64)
		objMin5.Voip, _ = strconv.ParseFloat(lstRecords[11], 64)

		if objMin5.Time == 0 {
			objMin5.Time = (nCurTime + 5) * 100
			objMin5.Open, _ = strconv.ParseFloat(lstRecords[2], 64)
			objMin5.High, _ = strconv.ParseFloat(lstRecords[3], 64)
			objMin5.Low, _ = strconv.ParseFloat(lstRecords[4], 64)
			objMin5.Amount, _ = strconv.ParseFloat(lstRecords[7], 64)
			objMin5.Volume, _ = strconv.ParseInt(lstRecords[8], 10, 64)
			objMin5.OpenInterest, _ = strconv.ParseInt(lstRecords[9], 10, 64)
			objMin5.NumTrades, _ = strconv.ParseInt(lstRecords[10], 10, 64)
			rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", objMin5.Date, objMin5.Time, objMin5.Open, objMin5.High, objMin5.Low, objMin5.Close, objMin5.Settle, objMin5.Amount, objMin5.Volume, objMin5.OpenInterest, objMin5.NumTrades, objMin5.Voip)
		}

		if objMin5.Time <= nCurTime*100 { // begin
			//if 0 != i {
			rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", objMin5.Date, objMin5.Time, objMin5.Open, objMin5.High, objMin5.Low, objMin5.Close, objMin5.Settle, objMin5.Amount, objMin5.Volume, objMin5.OpenInterest, objMin5.NumTrades, objMin5.Voip)
			//}

			objMin5.Time = (nCurTime + 5) * 100
			objMin5.Open, _ = strconv.ParseFloat(lstRecords[2], 64)
			objMin5.High, _ = strconv.ParseFloat(lstRecords[3], 64)
			objMin5.Low, _ = strconv.ParseFloat(lstRecords[4], 64)
			objMin5.Amount, _ = strconv.ParseFloat(lstRecords[7], 64)
			objMin5.Volume, _ = strconv.ParseInt(lstRecords[8], 10, 64)
			objMin5.OpenInterest, _ = strconv.ParseInt(lstRecords[9], 10, 64)
			objMin5.NumTrades, _ = strconv.ParseInt(lstRecords[10], 10, 64)
		} else {
			nHigh, _ := strconv.ParseFloat(lstRecords[3], 64)
			nLow, _ := strconv.ParseFloat(lstRecords[4], 64)
			if nHigh > objMin5.High {
				objMin5.High = nHigh
			}
			if nLow > objMin5.Low {
				objMin5.Low = nLow
			}
			nAmount, _ := strconv.ParseFloat(lstRecords[7], 64)
			objMin5.Amount += nAmount
			nVolume, _ := strconv.ParseInt(lstRecords[8], 10, 64)
			objMin5.Volume += nVolume
			nOpenInterest, _ := strconv.ParseInt(lstRecords[9], 10, 64)
			objMin5.OpenInterest += nOpenInterest
			nNumTrades, _ := strconv.ParseInt(lstRecords[10], 10, 64)
			objMin5.NumTrades += nNumTrades
		}

		if i == (nCount - 1) {
			rstr += fmt.Sprintf("%d,%d,%f,%f,%f,%f,%f,%f,%d,%d,%d,%f\n", objMin5.Date, objMin5.Time, objMin5.Open, objMin5.High, objMin5.Low, objMin5.Close, objMin5.Settle, objMin5.Amount, objMin5.Volume, objMin5.OpenInterest, objMin5.NumTrades, objMin5.Voip)
		}
	}

	return []byte(rstr), nReturnDate, len(bytesData)
}

///////////////////////// 1Minutes Lines ///////////////////////////////////////////
type Minutes1RecordIO struct {
	BaseRecordIO
}

func (pSelf *Minutes1RecordIO) CodeInWhiteTable(sFileName string) bool {
	if pSelf.CodeRangeFilter == nil {
		return true
	}

	nEnd := strings.LastIndexAny(sFileName, ".")
	nFileYear, err := strconv.Atoi(sFileName[nEnd-4 : nEnd])
	if nil != err {
		log.Println("[ERR] Minutes1RecordIO.CodeInWhiteTable() : Year In FileName is not digital: ", sFileName, nFileYear)
		return false
	}
	if time.Now().Year()-nFileYear >= 2 {
		return false
	}
	nBegin := strings.LastIndexAny(sFileName, "MIN")
	nEnd = nEnd - 5
	sCodeNum := sFileName[nBegin+1 : nEnd]

	return pSelf.CodeRangeFilter.CodeInRange(sCodeNum)
}

func (pSelf *Minutes1RecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	var nReturnDate int = -100
	var rstr string = ""
	var nOffset int = 0
	var objToday time.Time = time.Now()

	for _, bLine := range bytes.Split(bytesData, []byte("\n")) {
		nOffset += (len(bLine) + 1)
		sFirstFields := strings.Split(string(bLine), ",")[0]
		if len(sFirstFields) <= 0 {
			continue
		}
		nDate, err := strconv.Atoi(sFirstFields)
		if err != nil {
			continue
		}

		objRecordDate := time.Date(nDate/10000, time.Month(nDate%10000/100), nDate%100, 21, 6, 9, 0, time.Local)
		subHours := objToday.Sub(objRecordDate)
		nDays := subHours.Hours() / 24
		if nDays > 14 {
			continue
		}

		if -100 == nReturnDate {
			nReturnDate = nDate
		}

		if nReturnDate != nDate {
			return []byte(rstr), nReturnDate, nOffset
		}

		rstr += (string(bLine) + "\n")
	}

	return []byte(rstr), nReturnDate, len(bytesData)
}

///////////////////////// 1 Day Lines ///////////////////////////////////////////
type Day1RecordIO struct {
	BaseRecordIO
}

func (pSelf *Day1RecordIO) GetCompressLevel() int {
	return zlib.BestSpeed
}

func (pSelf *Day1RecordIO) GrapWriter(sFilePath string, nDate int, sSrcFile string) *tar.Writer {
	var sFile string = ""
	var objToday time.Time = time.Now()

	objRecordDate := time.Date(nDate/10000, time.Month(nDate%10000/100), nDate%100, 21, 6, 9, 0, time.Local)
	subHours := objToday.Sub(objRecordDate)
	nDays := subHours.Hours() / 24

	if nDays <= 16 { ////// Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate)
	} else { ////////////////////////// Not Current Month
		sFile = fmt.Sprintf("%s%d", sFilePath, nDate/10000*10000)
	}

	if objHandles, ok := pSelf.mapFileHandle[sFile]; ok {
		return objHandles.TarWriter
	} else {
		var objCompressHandles CompressHandles

		if true == objCompressHandles.OpenFile(sFile, pSelf.GetCompressLevel()) {
			pSelf.mapFileHandle[sFile] = objCompressHandles

			return pSelf.mapFileHandle[sFile].TarWriter
		} else {
			log.Println("[ERR] Day1RecordIO.GrapWriter() : failed 2 open *tar.Writer :", sFilePath)
		}

	}

	return nil
}

func (pSelf *Day1RecordIO) CodeInWhiteTable(sFileName string) bool {
	if pSelf.CodeRangeFilter == nil {
		return true
	}

	nBegin := strings.LastIndexAny(sFileName, "DAY")
	nEnd := strings.LastIndexAny(sFileName, ".")
	sCodeNum := sFileName[nBegin+1 : nEnd]

	return pSelf.CodeRangeFilter.CodeInRange(sCodeNum)
}

func (pSelf *Day1RecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	var nReturnDate int = -100
	var rstr string = ""
	var nOffset int = 0
	var nLastOffset int = 0
	var nReturnLen int = 0
	var bSep byte = byte('\n')
	var nBytesLen int = len(bytesData)

	for nOffset = 0; nOffset < nBytesLen; nOffset++ {
		if bytesData[nOffset] != bSep {
			continue
		}

		sLine := string(bytesData[nLastOffset:nOffset])
		lstRecords := strings.Split(sLine, ",")
		nReturnLen = nLastOffset
		nLastOffset = nOffset + 1
		sFirstFields := lstRecords[0]
		if len(sFirstFields) <= 0 {
			continue
		}
		nDate, err := strconv.Atoi(sFirstFields)
		if err != nil || 20180618 == nDate {
			continue
		}

		if -100 == nReturnDate {
			nReturnDate = nDate
		}

		if nReturnDate != nDate {
			return []byte(rstr), nReturnDate, nReturnLen
		}

		rstr += (string(sLine) + "\n")
	}

	return []byte(rstr), nReturnDate, nBytesLen
}

///////////////////////// Weights Lines ///////////////////////////////////////////
type WeightRecordIO struct {
	BaseRecordIO
}

func (pSelf *WeightRecordIO) LoadFromFile(bytesData []byte) ([]byte, int, int) {
	return bytesData, 20120609, len(bytesData)
}
