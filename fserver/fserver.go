/**
 * @brief		行情文件网络传输服务
 * @detail		Session管理 + 文件下载功能
 * @author		barry
 * @date		2018/4/10
 */
package fserver

import (
	"./github.com/astaxie/beego/session"
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	globalSessions *session.Manager = nil // 全局session管理对象
)

// Package Initialization
func init() {
	// 初始化session管理对象
	globalSessions, _ = session.NewManager("memory", &session.ManagerConfig{CookieName: "FileSyncSSID", EnableSetCookie: true, Gclifetime: 3600 * 10, Maxlifetime: 3600 * 10, Secure: false, CookieLifeTime: 3600 * 10, ProviderConfig: ""})
	go globalSessions.GC()
}

///////////////////////////////////// 资源下载清单类 //////////////////////////////////////

/**
 * @Class 		ResDownload
 * @brief		下载记录项目荐描述结构
 * @detail		用于描述待下载内容的类型，uri位置，校验码等信息
 * @author		barry
 */
type ResDownload struct {
	XMLName xml.Name `xml:"download"`
	TYPE    string   `xml:"type,attr"`
	URI     string   `xml:"uri,attr"`
	MD5     string   `xml:"md5,attr"`
	UPDATE  string   `xml:"update,attr"`
}

/**
 * @Class 		ResourceList
 * @brief		下载项列表汇总(ResDownload Table)
 * @detail		给下游客户端的下载资源列表（其中md5用于校验是否已经客户端下载过)
 * @author		barry
 */
type ResourceList struct {
	XMLName  xml.Name      `xml:"resource"`
	Download []ResDownload `xml:"download"`
}

///////////////////////////////////// 资源下载网络服务类 //////////////////////////////////////
/**
* @Class 		FileSyncServer
* @brief		资源下载网络服务
* @detail		支持的接口有 login/get/list:
				login:	用户登录
				list：	获取下载资源列表xml串
				get:	获取某一项在资源列表(xml)中的具体的资源数据
* @author		barry
*/
type FileSyncServer struct {
	ServerHost      string       // 被用户访问的 ip + port
	Account         string       // 登录帐号
	Password        string       // 登录密码
	SyncFolder      string       // 待下发的资源文件所在根目录
	objResourceList ResourceList // 待下发的资源文件的清单列表(对象,程序内部用，最终转换成sResponseList string)
	sResponseList   string       // 待下发的资源文件的清单列表(xml字符串)
	sSHM1RealPath   string       // 上海今日内实时1分钟数据存放目录(每n分钟生成一次，供quoteclientapi下载)
	sSZM1RealPath   string       // 深圳今日内实时1分钟数据存放目录(每n分钟生成一次，供quoteclientapi下载)
}

///< ---------------------- [Public 方法] -----------------------------
/**
* @brief		启动资源下载网络服务
* @detail		支持的接口有 login/get/list:
				login:	用户登录
				list：	获取下载资源列表xml串
				get:	获取某一项在资源列表(xml)中的具体的资源数据
* @note		配置了服务器 读超时 + 写超时
*/
func (pSelf *FileSyncServer) RunServer() {
	objSrv := &http.Server{
		Addr:         pSelf.ServerHost,
		ReadTimeout:  time.Second * 30 * 1,
		WriteTimeout: time.Second * 60 * 6,
	}

	// connections keep alive
	objSrv.SetKeepAlivesEnabled(true)
	// Create a http server && Register Http Event
	http.HandleFunc("/", pSelf.handleDefault)
	http.HandleFunc("/login", pSelf.handleLogin)
	http.HandleFunc("/get", pSelf.handleDownload)
	http.HandleFunc("/list", pSelf.handleList)

	// Active the http server
	log.Println("[INF] FileSyncServer.RunServer() : Sync Folder :", pSelf.SyncFolder)
	log.Println("[INF] FileSyncServer.RunServer() : Server Is Available [", pSelf.ServerHost, "] .........")
	objSrv.ListenAndServe()
	log.Println("[INF] FileSyncServer.RunServer() : Server Has Halted.........")
}

/**
 * @brief		获取上海实时1分钟线的数据的路径
 */
func (pSelf *FileSyncServer) GetSHRealMin1File() string {
	return pSelf.sSHM1RealPath
}

/**
* @brief		设置上海实时1分钟线的数据的路径
* @note			如果之前已经生成过一条1分钟线数据包，则会先删除之前的旧数据文件
				&&
				删除前会sleep一段时间(30秒)，以确保没有客户端在下载这个文件
*/
func (pSelf *FileSyncServer) SetSHRealMin1File(sMin1FilePath string) {
	var sOldFile string = pSelf.sSHM1RealPath

	pSelf.sSHM1RealPath = sMin1FilePath
	if sOldFile == "" {
		return
	}

	time.Sleep(time.Second * 15 * 2)
	err := os.Remove(sOldFile)
	if err != nil {
		log.Printf("[ERR] FileSyncServer.SetSHRealMin1File() : Error occur while removing (real)min1 file=%s : err=%s", sOldFile, err.Error())
	}
}

/**
 * @brief		获取深圳实时1分钟线的数据的路径
 */
func (pSelf *FileSyncServer) GetSZRealMin1File() string {
	return pSelf.sSZM1RealPath
}

/**
* @brief		设置深圳实时1分钟线的数据的路径
* @note			如果之前已经生成过一条1分钟线数据包，则会先删除之前的旧数据文件
				&&
				删除前会sleep一段时间(30秒)，以确保没有客户端在下载这个文件
*/
func (pSelf *FileSyncServer) SetSZRealMin1File(sMin1FilePath string) {
	var sOldFile string = pSelf.sSZM1RealPath

	pSelf.sSZM1RealPath = sMin1FilePath
	if sOldFile == "" {
		return
	}

	time.Sleep(time.Second * 15 * 2)
	err := os.Remove(sOldFile)
	if err != nil {
		log.Printf("[ERR] FileSyncServer.SetSHRealMin1File() : Error occur while removing (real)min1 file=%s : err=%s", sOldFile, err.Error())
	}
}

/**
 * @brief		更新下载资源列表信息(资源列表结构对象 + 资源xml字符串)
 * @detail		在旧结构中配对待更新的新列表，如果存在则直接更新，如果不存在则追加到末尾
 * @note		每次更新 "资源列表结构对象" 的同时，都会同步更新 "资源xml字符串"
 * @param[in]	refResList		新生成的资源列表结构
 */
func (pSelf *FileSyncServer) UpdateResList(refResList *ResourceList) {
	objNewResourceList := pSelf.objResourceList // clone一份当前的资源结构列表对象

	for _, objUpdateObject := range refResList.Download { // 遍历出每个待更新的资源
		var bFindUpdateItem bool = false // 是更新，还是追加标记

		for i, objResNode := range objNewResourceList.Download { // 遍历配对旧的资源列表
			if objResNode.TYPE == objUpdateObject.TYPE && objResNode.URI == objUpdateObject.URI {
				bFindUpdateItem = true                           // 能配对，标记为只更新，不追加
				objNewResourceList.Download[i] = objUpdateObject // 更新
			}
		}

		if false == bFindUpdateItem { // 未配对上，追加资源到列表
			objNewResourceList.Download = append(objNewResourceList.Download, objUpdateObject)
		}
	}

	pSelf.SetResList(&objNewResourceList) // 更新资源列表结构对象 + 生成新的xml资源列表字符串 + 存盘xml资源列表串（待盘中重启加载用）
}

/**
 * @brief		更新资源列表结构 和 资源xml字符串 并 存盘xml资源列表串（待盘中重启加载用）
 */
func (pSelf *FileSyncServer) SetResList(refResList *ResourceList) {
	pSelf.sResponseList = ""
	pSelf.objResourceList = *refResList

	//////////////////////////// 将资源列表结构转为xml串 /////////////////////////////////
	if sResponse, err := xml.Marshal(&pSelf.objResourceList); err != nil {
		log.Println("[ERR] FileSyncServer.SetResList() : Error Occur while marshaling xml obj. :", err.Error())
	} else {
		log.Println("[INF] FileSyncServer.SetResList() : marshaling xml obj. ...... ")
		pSelf.sResponseList = string(sResponse)

		//////////////////////// xml资源列表存盘 ./status.dat, 盘中启动服务时，从这个文件恢复
		objResponseSaver, err := os.Create("./restable.dat")
		defer objResponseSaver.Close()
		if nil != err {
			log.Println("[ERR] FileSyncServer.SetResList() : [FAILURE] cannot save ./restable.dat 2 disk :", err.Error())
		} else {
			nLen, _ := objResponseSaver.WriteString(pSelf.sResponseList)
			log.Printf("[INF] FileSyncServer.SetResList() : [OK] Write %d bytes 2 ./restable.dat", nLen)
		}
	}
}

/**
 * @brief		加载资源列表xml字符串 + 并从xml中恢复出资源列表结构对象
 */
func (pSelf *FileSyncServer) LoadResList() bool {
	objResponseLoader, err := os.Open("./restable.dat")
	defer objResponseLoader.Close()
	if nil == err {
		bytesData := make([]byte, 1024*1024*8)
		nLen, _ := objResponseLoader.Read(bytesData)
		pSelf.sResponseList = string(bytesData[:nLen]) // 恢复xml资源列表串

		err = xml.Unmarshal([]byte(pSelf.sResponseList), &(pSelf.objResourceList)) // 从xml恢复出资源列表结构对象
		if err != nil {
			log.Println("[ERR] FileSyncServer.LoadResList() : [ERR] cannot unmarshal xml string in ./restable.dat : ", err.Error())
			return false
		}

		log.Printf("[INF] FileSyncServer.LoadResList() : [OK] load %d bytes from ./restable.dat && Resources Number = %d", nLen, len(pSelf.objResourceList.Download))

		return true
	}

	log.Println("[ERR] FileSyncServer.LoadResList() : [ERR] cannot load ./restable.dat : ", err.Error())

	return false
}

///< ---------------------- [Private 方法] -----------------------------
/**
 * @brief		用户信息认证接口
 * @detail		判断某个session是否存在
 */
func (pSelf *FileSyncServer) authenticateSession(resp http.ResponseWriter, req *http.Request) bool {
	req.ParseForm()
	objSession, _ := globalSessions.SessionStart(resp, req)
	defer objSession.SessionRelease(resp)
	sUNameInSS := objSession.Get("username")

	if sUNameInSS == nil {
		var xmlRes struct {
			XMLName xml.Name `xml:"authenticate"`
			Result  struct {
				XMLName xml.Name `xml:"result"`
				Status  string   `xml:"status,attr"`
				Desc    string   `xml:"desc,attr"`
			}
		} // Build Response Xml Structure

		xmlRes.Result.Status = "failure"
		xmlRes.Result.Desc = "[WARNING] Oops! user session has expired."
		log.Println("[INF] [AuthenticateUser] ---> [FAILURE]")

		// Marshal Obj 2 Xml String && Write 2 HTTP Response Object
		if sResponse, err := xml.Marshal(&xmlRes); err != nil {
			fmt.Fprintf(resp, "%s", err.Error())
		} else {
			fmt.Fprintf(resp, "%s%s", xml.Header, string(sResponse))
		}

		return false
	}

	return true
}

/**
 * @brief		帮助接口
 */
func (pSelf *FileSyncServer) handleDefault(resp http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(resp, "Server Of File Sync Program.\n\nUsage Of Action:\n\nhttp://127.0.0.1/login?account=xx&password=xxx\n\nhttp://127.0.0.1/get?uri=xxx.zip\n\nhttp://127.0.0.1/list\n\n")
}

/**
 * @brief		用户登录接口
 * @detail		用于验证登录帐号和密码是否正确，通过后，新增一个合法的session
 */
func (pSelf *FileSyncServer) handleLogin(resp http.ResponseWriter, req *http.Request) {
	var sAccount, sPswd string // LoginName && LoginPassword
	var xmlRes struct {
		XMLName xml.Name `xml:"login"`
		Result  struct {
			XMLName xml.Name `xml:"result"`
			Status  string   `xml:"status,attr"`
			Desc    string   `xml:"desc,attr"`
		}
	} // Build Response Xml Structure

	// Initialize Arguments
	req.ParseForm()
	objSession, _ := globalSessions.SessionStart(resp, req)
	defer objSession.SessionRelease(resp)
	sUNameInSS := objSession.Get("username")

	// Check Login Status
	if sUNameInSS != nil {
		xmlRes.Result.Status = "success"
		xmlRes.Result.Desc = "[INFO] welcome again"
		log.Println("[INF] HttpAction[Relogin], [OK]: ", sUNameInSS)
	} else {
		// Fetch Aruguments ( LoginName && LoginPassword )
		xmlRes.Result.Status = "failure"
		xmlRes.Result.Desc = "[WARNING] Oops! account or password r incorrect."
		if len(req.Form["account"]) > 0 {
			sAccount = req.Form["account"][0]
		}

		if len(req.Form["password"]) > 0 {
			sPswd = req.Form["password"][0]
		}

		// Check LoginName && LoginPassword
		if pSelf.Account == sAccount && pSelf.Password == sPswd {
			objSession.Set("username", sAccount)
			xmlRes.Result.Status = "success"
			xmlRes.Result.Desc = "[INFO] Good! account and password r all correct."
			log.Println("[INF] HttpAction[Login], [OK]: ", sAccount)
		} else {
			log.Println("[INF] HttpAction[Login], [FAILED]: ", sAccount)
		}
	}

	// Marshal Obj 2 Xml String && Write 2 HTTP Response Object
	if sResponse, err := xml.Marshal(&xmlRes); err != nil {
		fmt.Fprintf(resp, "%s", err.Error())
	} else {
		fmt.Fprintf(resp, "%s%s", xml.Header, string(sResponse))
	}
}

/**
 * @brief		根据请求的uri信息，判断是否需要做重定向，用于获取正确的资源位置
 * @note		比如： 获取沪、深今日内的实时1分钟线资源包
 */
func (pSelf *FileSyncServer) redirectURI(sFileName string) string {
	if strings.Contains(sFileName, "MIN1_TODAY") == true {
		if strings.Contains(sFileName, "SSE") == true {
			return pSelf.sSHM1RealPath // 获取上海的实时1分钟线资源包
		}

		if strings.Contains(sFileName, "SZSE") == true {
			return pSelf.sSZM1RealPath // 获取深圳的实时1分钟线资源包
		}
	}

	return sFileName
}

/**
 * @brief		资源列表xml中的具体某一项资源的下载接口
 */
func (pSelf *FileSyncServer) handleDownload(resp http.ResponseWriter, req *http.Request) {
	var sZipName string = ""
	var xmlRes struct {
		XMLName xml.Name `xml:"download"`
		Result  struct {
			XMLName xml.Name `xml:"result"`
			Status  string   `xml:"status,attr"`
			Desc    string   `xml:"desc,attr"`
		}
	} // Build Response Xml Structure

	if pSelf.authenticateSession(resp, req) == false {
		return
	}

	// Initialize Arguments
	req.ParseForm()

	// Download Zip File
	if len(req.Form["uri"]) > 0 {
		sZipName = pSelf.redirectURI(req.Form["uri"][0])
		resp.Header().Set("Content-Type", "application/zip")
		resp.Header().Set("Content-Encoding", "zip")
		resp.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", sZipName))
		dataRes, err := ioutil.ReadFile(sZipName)
		if err == nil {
			resp.Write(dataRes)
		} else {
			xmlRes.Result.Status = "failure"
			xmlRes.Result.Desc = "[WARNING] Oops! failed 2 load data file," + sZipName
			// Marshal Obj 2 Xml String && Write 2 HTTP Response Object
			if sResponse, err := xml.Marshal(&xmlRes); err != nil {
				fmt.Fprintf(resp, "%s", err.Error())
			} else {
				fmt.Fprintf(resp, "%s%s", xml.Header, string(sResponse))
			}
		}
	} else {
		xmlRes.Result.Status = "failure"
		xmlRes.Result.Desc = "[WARNING] Oops! miss argument, GET: uri=''"
		log.Println("[INF] [Download File] ---> [FAILURE], miss argument, GET: uri='nil'")

		// Marshal Obj 2 Xml String && Write 2 HTTP Response Object
		if sResponse, err := xml.Marshal(&xmlRes); err != nil {
			fmt.Fprintf(resp, "%s", err.Error())
		} else {
			fmt.Fprintf(resp, "%s%s", xml.Header, string(sResponse))
		}
	}
}

/**
 * @brief		返回资源列表xml字符串
 */
func (pSelf *FileSyncServer) handleList(resp http.ResponseWriter, req *http.Request) {
	if pSelf.authenticateSession(resp, req) == false {
		return
	}

	fmt.Fprintf(resp, "%s%s", xml.Header, []byte(pSelf.sResponseList))
}
