// program entry ...............
package main

import (
	"./fserver"
	"flag"
	"log"
	"os"
)

var (
	sIP      string // Server IP
	nPort    int    // Server Port
	bDumpLog bool   // Switch 4 Log Dump
	sLogFile string // Log File Path
	sXmlCfg  string // Configuration File (.xml)
)

// package imitialization
func init() {
	/////////////// Parse Arguments From Command Line
	flag.IntVar(&nPort, "port", 31256, "file sync server's listen port (default:31256)")
	flag.StringVar(&sIP, "ip", "127.0.0.1", "file sync server's ip address (default:127.0.0.1)")
	flag.BoolVar(&bDumpLog, "dumplog", false, "a switch 4 log dump (default:false)")
	flag.StringVar(&sLogFile, "logpath", "./server.log", "log file's path (default:./Server.log)")
	flag.StringVar(&sXmlCfg, "cfg", "./server.xml", "configuration of server (.xml)")
	flag.Parse()
}

// program entry function
func main() {
	/////////////// Set Log File Path
	if true == bDumpLog {
		oLogFile, oLogErr := os.OpenFile(sLogFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		if oLogErr != nil {
			log.Fatal("[ERR] main() : an error occur while creating log file ! ", sLogFile)
			os.Exit(1)
		}

		log.SetOutput(oLogFile)
	}

	//////////////// Active File Sync Server
	log.Println("[INF] [Begin] ##################################")
	log.Println("[INF] Server IP:Port -->", sIP, nPort)
	log.Println("[INF] Server Configuration Path -->", sXmlCfg)

	fserver.RunServer()

	log.Println("[INF] [ End ] ##################################")
}
