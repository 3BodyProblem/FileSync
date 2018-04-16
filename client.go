/**
 * @brief		entry file of program
 * @detail		files sync client (milti-thread)
 * @author		barry
 * @date		2018/4/10
 */
package main

import (
	"./fclient"
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
)

// Package Initialization
func init() {
	/////////////// Parse Arguments From Command Line
	// [Optional]
	flag.StringVar(&sIP, "ip", "127.0.0.1", "file sync server's ip address (default:0.0.0.0)")
	flag.IntVar(&nPort, "port", 31256, "file sync server's listen port (default:31256)")
	flag.StringVar(&sLogFile, "logpath", "./Client.log", "log file's path (default:./Client.log)")
	flag.BoolVar(&bDumpLog, "dumplog", false, "a switch 4 log dump (default:false)")
	// [Mandatory]
	flag.StringVar(&sAccount, "account", "", "login user name (default: '' ")
	flag.StringVar(&sPassword, "password", "", "login password () default : '' ")
	flag.Parse()
}

// Program Entry Function
func main() {
	/////////////// Set Log File Path
	if true == bDumpLog {
		oLogFile, oLogErr := os.OpenFile(sLogFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		if oLogErr != nil {
			log.Fatal("[ERR] main() : a fatal error occur while creating log file ! ", sLogFile)
		}

		log.SetOutput(oLogFile)
	}

	//////////////// Declare && Active FileSync Server / File Scheduler
	log.Println("[INF] [Begin] ##################################")

	objSyncClient := &fclient.FileSyncClient{ServerHost: fmt.Sprintf("%s:%d", sIP, nPort), Account: sAccount, Password: sPassword}
	objSyncClient.DoTasks()

	log.Println("[INF] [ End ] ##################################")
}