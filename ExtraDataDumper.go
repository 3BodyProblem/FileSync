/**
 * @brief		entry file of program
 * @detail		extra data from taiwan ftp
 * @author		barry
 * @date		2018/4/10
 */
package main

import (
	"./goftp"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	sIP        string // Server IP
	nPort      int    // Server Port
	nTTL       int    // Time To Live (default: 3600 second)
	bDumpLog   bool   // Switch 4 Log Dump
	sLogFile   string // Log File Path
	sAccount   string // Login Name
	sPassword  string // Login Password
	sTmpFolder string // Folder Which Extract Data
)

// Package Initialization
func init() {
	/////////////// Parse Arguments From Command Line
	// [Optional]
	flag.StringVar(&sIP, "ip", "59.120.27.116", "ftp server's ip address (default:59.120.27.166)")
	flag.IntVar(&nPort, "port", 40021, "ftp server's listen port (default:40021)")
	flag.IntVar(&nTTL, "ttl", 3600*6, " (time to live (default: 3600 * 6 seconds)")
	flag.StringVar(&sLogFile, "logpath", "./FtpData.log", "log file's path (default:./FtpData.log)")
	flag.BoolVar(&bDumpLog, "dumplog", false, "a switch 4 log dump (default:false)")
	flag.StringVar(&sTmpFolder, "dir", "./FtpCache/", "data folder path (default :./FtpCache/)")
	// [Mandatory]
	flag.StringVar(&sAccount, "account", "", "login user name (default: '' ")
	flag.StringVar(&sPassword, "password", "", "login password () default : '' ")
	flag.Parse()
}

// Program Entry Function
func main() {
	//////////////// Set Log File Path ///////////////////////////////////////////
	if true == bDumpLog {
		oLogFile, oLogErr := os.OpenFile(sLogFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
		if oLogErr != nil {
			log.Fatal("[ERR] main() : a fatal error occur while creating log file ! ", sLogFile)
		}

		log.SetOutput(oLogFile)
	}

	sTmpFolder = strings.Replace(sTmpFolder, "\\", "/", -1)
	sTmpFolder := sTmpFolder[:strings.LastIndex(sTmpFolder, "/")]
	sTmpFolder = fmt.Sprintf("%s.%d", sTmpFolder, time.Now().Unix())
	sTmpFolder += "/"
	log.Println("[INF] [Ver] ######### 1.0.1 ####################")
	log.Println("[INF] [Begin] ##################################")
	/////////////// Ftp ///////////////////////////////////////////////////////////
	var err error
	var ftp *goftp.FTP
	var sUrl string = fmt.Sprintf("%s:%d", sIP, nPort)
	/////////////// For debug messages: goftp.ConnectDbg(sUrl) ////////////////////
	if ftp, err = goftp.Connect(sUrl); err != nil {
		log.Println("[ERR] an error occur while connecting ftp server :", err.Error())
		return
	}
	defer ftp.Close()
	log.Println("[INF] Ftp Connection has been established! --> ", sUrl)
	/////////////// Username / password authentication ////////////////////////////
	if err = ftp.Login(sAccount, sPassword); err != nil {
		log.Println("[ERR] invalid username or password :", err.Error())
		return
	}
	if err = ftp.Cwd("/"); err != nil {
		log.Println("[ERR] cannot walk 2 root folder of FTP Server :", err.Error())
		return
	}
	log.Println("[INF] Root -->", sTmpFolder)
	err = os.MkdirAll(sTmpFolder, 0755)
	if err != nil {
		log.Println("[ERR] cannot build root folder : ", sTmpFolder, err.Error())
		return
	}

	/////////////// Download ./Participant.txt ////////////////////////////////////
	_, err = ftp.Retr("./Participant.txt", func(r io.Reader) error {
		sLocalFile := filepath.Join(sTmpFolder, "Participant.txt")
		fw, err := os.OpenFile(sLocalFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Println("[ERR] cannot create file, file name =", sLocalFile, err.Error())
			return err
		}

		_, err = io.Copy(fw, r)
		if err != nil {
			log.Println("[ERR] cannot write 2 file, file name =", sLocalFile, err.Error())
			fw.Close()
			return err
		}

		fw.Close()
		return err
	})
	log.Println("[INF] [Done] Ftp File : ./Participant.txt")
	/////////////// Download shase_rzrq_by_date ////////////////////////////////////
	sLocalFolder := filepath.Join(sTmpFolder, "shase_rzrq_by_date/")
	err = os.MkdirAll(sLocalFolder, 0755)
	if err != nil {
		log.Println("[ERR] cannot build shase_rzrq_by_date/ folder : ", sLocalFolder, err.Error())
		return
	}
	err = ftp.Walk("/shase_rzrq_by_date/", func(path string, info os.FileMode, err error) error {
		_, err = ftp.Retr(path, func(r io.Reader) error {
			path = strings.Replace(path, "\\", "/", -1)
			sFileName := path[strings.LastIndex(path, "/"):]
			sLocalFile := filepath.Join(sLocalFolder, sFileName)

			fw, err := os.OpenFile(sLocalFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				log.Println("[ERR] cannot create file, file name =", sLocalFile, err.Error())
				return err
			}

			_, err = io.Copy(fw, r)
			if err != nil {
				log.Println("[ERR] cannot write 2 file, file name =", sLocalFile, err.Error())
				fw.Close()
				return err
			}

			fw.Close()

			return err
		})

		return nil
	})
	log.Println("[INF] [Done] Ftp File : ./shase_rzrq_by_date/")
	/////////////// Download sznse_rzrq_by_date ////////////////////////////////////
	sLocalFolder = filepath.Join(sTmpFolder, "sznse_rzrq_by_date/")
	err = os.MkdirAll(sLocalFolder, 0755)
	if err != nil {
		log.Println("[ERR] cannot build sznse_rzrq_by_date/ folder : ", sLocalFolder, err.Error())
		return
	}
	err = ftp.Walk("/sznse_rzrq_by_date/", func(path string, info os.FileMode, err error) error {
		_, err = ftp.Retr(path, func(r io.Reader) error {
			path = strings.Replace(path, "\\", "/", -1)
			sFileName := path[strings.LastIndex(path, "/"):]
			sLocalFile := filepath.Join(sLocalFolder, sFileName)

			fw, err := os.OpenFile(sLocalFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				log.Println("[ERR] cannot create file, file name =", sLocalFile, err.Error())
				return err
			}

			_, err = io.Copy(fw, r)
			if err != nil {
				log.Println("[ERR] cannot write 2 file, file name =", sLocalFile, err.Error())
				fw.Close()
				return err
			}

			fw.Close()

			return err
		})

		return nil
	})
	log.Println("[INF] [Done] Ftp File : ./sznse_rzrq_by_date/")
	/////////////// Download shsz_idx_by_date/ ////////////////////////////////////
	sLocalFolder = filepath.Join(sTmpFolder, "shsz_idx_by_date/")
	err = os.MkdirAll(sLocalFolder, 0755)
	if err != nil {
		log.Println("[ERR] cannot build shsz_idx_by_date/ folder : ", sLocalFolder, err.Error())
		return
	}
	err = ftp.Walk("/shsz_idx_by_date/", func(path string, info os.FileMode, err error) error {
		_, err = ftp.Retr(path, func(r io.Reader) error {
			path = strings.Replace(path, "\\", "/", -1)
			sFileName := path[strings.LastIndex(path, "/"):]
			sLocalFile := filepath.Join(sLocalFolder, sFileName)

			fw, err := os.OpenFile(sLocalFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				log.Println("[ERR] cannot create file, file name =", sLocalFile, err.Error())
				return err
			}

			_, err = io.Copy(fw, r)
			if err != nil {
				log.Println("[ERR] cannot write 2 file, file name =", sLocalFile, err.Error())
				fw.Close()
				return err
			}

			fw.Close()

			return err
		})

		return nil
	})
	log.Println("[INF] [Done] Ftp File : ./shsz_idx_by_date/")
	/////////////// Download shsz_detail/ ////////////////////////////////////
	sLocalFolder = filepath.Join(sTmpFolder, "shsz_detail/")
	err = os.MkdirAll(sLocalFolder, 0755)
	if err != nil {
		log.Println("[ERR] cannot build shsz_detail/ folder : ", sLocalFolder, err.Error())
		return
	}
	err = ftp.Walk("/shsz_detail/", func(path string, info os.FileMode, err error) error {
		_, err = ftp.Retr(path, func(r io.Reader) error {
			path = strings.Replace(path, "\\", "/", -1)
			sFileName := path[strings.LastIndex(path, "/"):]
			sLocalFile := filepath.Join(sLocalFolder, sFileName)

			fw, err := os.OpenFile(sLocalFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				log.Println("[ERR] cannot create file, file name =", sLocalFile, err.Error())
				return err
			}

			_, err = io.Copy(fw, r)
			if err != nil {
				log.Println("[ERR] cannot write 2 file, file name =", sLocalFile, err.Error())
				fw.Close()
				return err
			}

			fw.Close()

			return err
		})

		return nil
	})
	log.Println("[INF] [Done] Ftp File : ./shsz_detail/")

	os.RemoveAll("./HKSE")
	err = os.Rename(sTmpFolder, "./HKSE/")
	if err != nil {
		log.Println("[ERR] cannot remove folder, folder name =", sTmpFolder, err.Error())
		return
	}

	log.Println("[INF] [ End ] ##################################")
}
