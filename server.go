package main

import (
	"flag"
	"log"
	"os"
)

var (
	sIP   string
	nPort int
)

func init() {
	// parse arguments from command line
	flag.IntVar(&nPort, "port", 31256, "listen port")
	flag.StringVar(&sIP, "ip", "127.0.0.1", "server ip address")
	flag.Parse()
}

func main() {
	// set log file
	oLogFile, oLogErr := os.OpenFile("./Server.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if oLogErr != nil {
		log.Fatal("main() : an error occur while creating log file !")
		os.Exit(1)
	}

	log.SetOutput(oLogFile)
	log.Println("[Begin] ##################################")
	log.Println("[INF] Server IP:port -->", sIP, nPort)

	log.Println("[ End ] ##################################")
}
