/**
 * @brief		entry file of program
 * @detail		files sync server
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
	sIP         string // Server IP
	nPort       int    // Server Port
	bDumpLog    bool   // Switch 4 Log Dump
	sLogFile    string // Log File Path
	sSyncFolder string // Sync File Folder
	sAccount    string // Login Name
	sPassword   string // Login Password
)

// Package Initialization
func init() {
	/////////////// Parse Arguments From Command Line
	flag.IntVar(&nPort, "port", 31256, "file sync server's listen port (default:31256)")
	flag.StringVar(&sIP, "ip", "127.0.0.1", "file sync server's ip address (default:127.0.0.1)")
	flag.BoolVar(&bDumpLog, "dumplog", false, "a switch 4 log dump (default:false)")
	flag.StringVar(&sLogFile, "logpath", "./server.log", "log file's path (default:./Server.log)")
	flag.StringVar(&sSyncFolder, "cfg", "./SyncFolder/", "data folder 4 sync")
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
			log.Fatal("[ERR] main() : an error occur while creating log file ! ", sLogFile)
			os.Exit(1) // abort
		}

		log.SetOutput(oLogFile)
	}

	//////////////// Declare && Active File Sync Server
	log.Println("[INF] [Begin] ##################################")
	log.Println("[INF] Sync Folder -->", sSyncFolder)

	objSyncSvr := &fserver.FileSyncServer{ServerHost: fmt.Sprintf("%s:%d", sIP, nPort), Account: sAccount, Password: sPassword}
	objSyncSvr.RunServer()

	log.Println("[INF] [ End ] ##################################")
}
