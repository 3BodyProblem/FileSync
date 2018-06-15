/**
 * @brief		服务器文件入口
 * @detail		功能： 命令行参数管理 + NetService + 行情数据文件生成服务 初始化
 * @author		barry
 * @date		2018/4/10
 */
package main

import (
	"./fserver"
	"flag"
	"fmt"
	"log"
	"os"
)

var (
	sIP       string // Server IP
	nPort     int    // Server Port
	bDumpLog  bool   // Switch 4 Log Dump
	sLogFile  string // Log File Path
	sAccount  string // Login Name
	sPassword string // Login Password
	sXmlCfg   string // Xml Configuration Path
)

// Package Initialization
func init() {
	/////////////// Parse Arguments From Command Line
	// [Optional]
	flag.StringVar(&sIP, "ip", "0.0.0.0", "file sync server's ip address (default:0.0.0.0)")
	flag.IntVar(&nPort, "port", 31256, "file sync server's listen port (default:31256)")
	flag.StringVar(&sLogFile, "logpath", "./Server.log", "log file's path (default:./Server.log)")
	flag.BoolVar(&bDumpLog, "dumplog", false, "a switch 4 log dump (default:false)")
	// [Mandatory]
	flag.StringVar(&sXmlCfg, "cfg", "./cfg/configuration.xml", "configuration 4 files sync scheduler")
	flag.StringVar(&sAccount, "account", "", "login user name (default: '' ")
	flag.StringVar(&sPassword, "password", "", "login password () default : '' ")
	flag.Parse()
}

// Program Entry Function
func main() {
	/////////////// 设置日志输出方式
	if true == bDumpLog {
		oLogFile, oLogErr := os.OpenFile(sLogFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		if oLogErr != nil {
			log.Fatal("[ERR] main() : a fatal error occur while creating log file ! ", sLogFile)
		}

		log.SetOutput(oLogFile)
	}

	//////////////// 启动 网络传输服务(fserver.FileSyncServer) + 行情数据文件生成服务(fserver.FileScheduler)
	log.Println("[INF] [Ver] ######## 1.0.1 #####################")
	log.Println("[INF] [Begin] ##################################")

	objFileScheduler := &fserver.FileScheduler{XmlCfgPath: sXmlCfg}
	objSyncSvr := &fserver.FileSyncServer{ServerHost: fmt.Sprintf("%s:%d", sIP, nPort), Account: sAccount, Password: sPassword}

	objFileScheduler.RefSyncSvr = objSyncSvr
	if objFileScheduler.Active() == false {
		log.Fatal("[ERR] main() : a fatal error occur while initialize file scheduler engine ! ")
	} else {
		objSyncSvr.SyncFolder = objFileScheduler.SyncFolder
		objSyncSvr.RunServer()
	}

	log.Println("[INF] [ End ] ##################################")
}
