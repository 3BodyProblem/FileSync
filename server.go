package main


import (
	"os"
	"log"
	"fmt"
	"flag"
)


func init() {

}


func main() {
	// set log file
	oLogFile, oLogErr := os.OpenFile( "./Server.log", os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666 )
	if oLogErr != nil {
		log.Fatal( "main() : an error occur while creating log file !" )
		os.Exit( 1 )
	}

	flag.Parse()
	log.SetOutput( oLogFile )
	log.Println( "[Begin] ##################################" )

	// parse arguments
	pszIP := flag.String( "ip", "127.0.0.1", "ip address" )
	pnPort := flag.Int( "port", 31256, "port" )

	fmt.Println( *pszIP, *pnPort )

	log.Println( "[ End ] ##################################" )
}




