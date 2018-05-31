/**
 * @brief		qianlong ftp resource manager class
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func init() {
}

func Min(x, y int) int {
	if x < y {
		return x
	}

	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}

	return y
}

func parseTimeStr(sTimeString string) (int, int, int, int, int, int, bool) {
	lstDateTime := strings.Split(sTimeString, " ")
	lstDate := strings.Split(lstDateTime[0], "-")
	lstTime := strings.Split(lstDateTime[1], ":")

	nYY, err := strconv.Atoi(lstDate[0])
	if nil != err {
		log.Println("[WARN] parseTimeStr() : cannot parse Year :", lstDate[0], err.Error())
		return 0, 0, 0, 0, 0, 0, false
	}

	nMM, err := strconv.Atoi(lstDate[1])
	if nil != err {
		log.Println("[WARN] parseTimeStr() : cannot parse Month :", lstDate[1], err.Error())
		return 0, 0, 0, 0, 0, 0, false
	}

	nDD, err := strconv.Atoi(lstDate[2])
	if nil != err {
		log.Println("[WARN] parseTimeStr() : cannot parse Day :", lstDate[0], err.Error())
		return 0, 0, 0, 0, 0, 0, false
	}

	nHH, err := strconv.Atoi(lstTime[0])
	if nil != err {
		log.Println("[WARN] parseTimeStr() : cannot parse Hour :", lstTime[0], err.Error())
		return 0, 0, 0, 0, 0, 0, false
	}

	nmm, err := strconv.Atoi(lstTime[1])
	if nil != err {
		log.Println("[WARN] parseTimeStr() : cannot parse Minute :", lstTime[1], err.Error())
		return 0, 0, 0, 0, 0, 0, false
	}

	nSS, err := strconv.Atoi(lstTime[2][:2])
	if nil != err {
		log.Println("[WARN] parseTimeStr() : cannot parse Second :", lstTime[2], err.Error())
		return 0, 0, 0, 0, 0, 0, false
	}

	return nYY, nMM, nDD, nHH, nmm, nSS, true
}

//////////// Download Qianlong FTP Resource Files ///////////////////////////////////////////

func SyncQLFtpFilesInPeriodTime(nBeginTime int, nEndTime int) bool {
	var nNowTime int = int(time.Now().Hour())*10000 + int(time.Now().Minute())*100 + int(time.Now().Second())

	if nNowTime >= nBeginTime && nNowTime <= nEndTime {
		log.Printf("[INF] SyncQLFtpFilesInPeriodTime( %d, %d ) : execute: (%d) ExtraDataDumper.bat", nBeginTime, nEndTime, nNowTime)
		cmd := exec.Command("ExtraDataDumper.bat")
		if err := cmd.Run(); err != nil {
			log.Printf("[ERR] SyncQLFtpFilesInPeriodTime( %d, %d ) : error info: %v", nBeginTime, nEndTime, err)
			return false
		}

		log.Printf("[INF] SyncQLFtpFilesInPeriodTime( %d, %d ) : executed!", nBeginTime, nEndTime)

		return true
	}

	return false
}
