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
	flag.BoolVar(&bDumpLog, "dumplog", true, "a switch 4 log dump (default:false)")
	flag.StringVar(&sTmpFolder, "dir", "./HKSE/", "data folder path (default :./HKSE/)")
	// [Mandatory]
	flag.StringVar(&sAccount, "account", "", "login user name (default: '' ")
	flag.StringVar(&sPassword, "password", "", "login password () default : '' ")
	flag.Parse()
	// Generate Folder Name
	sTmpFolder = strings.Replace(sTmpFolder, "\\", "/", -1)[:strings.LastIndex(sTmpFolder, "/")]
}

// FTP Folder Downloader
type FTPFolderSync struct {
	SpecifyFile  string     // Download Specify File
	LocalFolder  string     // Download Folder
	FTPResFolder string     // FTP Resources Folder
	FTPHandlePtr *goftp.FTP // FTP Handle Pointer
}

func (pSelf *FTPFolderSync) FilesSync() int {
	var err error
	var nFetchCount int = 0
	// Build Dirs
	err = os.MkdirAll(pSelf.LocalFolder, 0755)
	if err != nil {
		log.Println("[ERR] FTPFolderSync::FilesSync() : cannot build folder : ", pSelf.LocalFolder, err.Error())
		return nFetchCount
	}
	// Download Single File
	if pSelf.SpecifyFile != "" {
		sFilePath := strings.Replace(filepath.Join(pSelf.FTPResFolder, pSelf.SpecifyFile), "\\", "/", -1)
		sLocalFile := strings.Replace(filepath.Join(pSelf.LocalFolder, pSelf.SpecifyFile), "\\", "/", -1)
		log.Printf("[INF] FTPFolderSync::FilesSync() : downloading file : %s --> %s", sFilePath, sLocalFile)

		_, err = pSelf.FTPHandlePtr.Retr(sFilePath, func(r io.Reader) error {
			fw, err := os.OpenFile(sLocalFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
			if err != nil {
				log.Println("[ERR] FTPFolderSync::FilesSync() : cannot create file, file name =", sLocalFile, err.Error())
				return nil
			}

			_, err = io.Copy(fw, r)
			if err != nil {
				log.Println("[ERR] FTPFolderSync::FilesSync() : cannot write 2 file, file name =", sLocalFile, err.Error())
				fw.Close()
				return nil
			}

			fw.Close()
			nFetchCount += 1
			log.Printf("[Done] FTPFolderSync::FilesSync() : Ftp File : FTP[%s] ---> LOCAL[%s], Fetch %d Files", sFilePath, sLocalFile, nFetchCount)

			return err
		})

		return nFetchCount
	}
	// Walk Ftp && Download
	err = pSelf.FTPHandlePtr.Walk(pSelf.FTPResFolder, func(path string, info os.FileMode, err error) error {
		path = strings.Replace(path, "\\", "/", -1)
		sFileName := path[strings.LastIndex(path, "/"):]
		sLocalFile := filepath.Join(pSelf.LocalFolder, sFileName)
		sLocalFile = strings.Replace(sLocalFile, "\\", "/", -1)

		//log.Println(path, sLocalFile)
		_, err = os.Stat(sLocalFile)
		if err != nil {
			if os.IsNotExist(err) {
				_, err = pSelf.FTPHandlePtr.Retr(path, func(r io.Reader) error {
					fw, err := os.OpenFile(sLocalFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
					if err != nil {
						log.Println("[ERR] FTPFolderSync::FilesSync() : cannot create file, file name =", sLocalFile, err.Error())
						os.Exit(-100)
					}

					_, err = io.Copy(fw, r)
					if err != nil {
						log.Println("[ERR] FTPFolderSync::FilesSync() : cannot write 2 file, file name =", sLocalFile, err.Error())
						fw.Close()
						os.Exit(-100)
					}

					fw.Close()
					nFetchCount += 1

					return err
				})

				return nil
			}
		}
		//log.Printf("[INF] FTPFolderSync::FilesSync() : ignore file : %s --> %s", path, sLocalFile)
		return nil
	})

	log.Printf("[Done] FTPFolderSync::FilesSync() : Ftp File : FTP[%s] ---> LOCAL[%s], Fetch %d Files", pSelf.FTPResFolder, pSelf.LocalFolder, nFetchCount)

	return nFetchCount
}

func (pSelf *FTPFolderSync) SyncBeforeTime(nTime int /*650-->6:50*/) int {
	nHour := int(time.Now().Hour())
	nMinute := int(time.Now().Minute())
	nNowT := nHour*100 + nMinute

	if nNowT < nTime {
		return pSelf.FilesSync()
	} else {
		return 0
	}
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
	objParticipantSync := FTPFolderSync{LocalFolder: sTmpFolder, FTPResFolder: "/", SpecifyFile: "Participant.txt", FTPHandlePtr: ftp}
	objParticipantSync.SyncBeforeTime(650)
	/////////////// Download shase_rzrq_by_date ////////////////////////////////////
	objSHRzrqSync := FTPFolderSync{LocalFolder: filepath.Join(sTmpFolder, "shase_rzrq_by_date/"), FTPResFolder: "/shase_rzrq_by_date/", FTPHandlePtr: ftp}
	objSHRzrqSync.FilesSync()
	/////////////// Download sznse_rzrq_by_date ////////////////////////////////////
	objSZRzrqSync := FTPFolderSync{LocalFolder: filepath.Join(sTmpFolder, "sznse_rzrq_by_date/"), FTPResFolder: "/sznse_rzrq_by_date/", FTPHandlePtr: ftp}
	objSZRzrqSync.FilesSync()
	/////////////// Download shsz_idx_by_date/ ////////////////////////////////////
	objSHSZIndexSync := FTPFolderSync{LocalFolder: filepath.Join(sTmpFolder, "shsz_idx_by_date/"), FTPResFolder: "/shsz_idx_by_date/", FTPHandlePtr: ftp}
	objSHSZIndexSync.FilesSync()
	/////////////// Download shsz_detail/ ////////////////////////////////////
	objSHSZDetailSync := FTPFolderSync{LocalFolder: filepath.Join(sTmpFolder, "shsz_detail/"), FTPResFolder: "/shsz_detail/", FTPHandlePtr: ftp}
	objSHSZDetailSync.FilesSync()

	log.Println("[INF] [ End ] ##################################")
}
