package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"net/http"
	"os"
	"runtime"
	"github.com/wuqtao/chat-room-server/server"
	"github.com/wuqtao/chat-room-server/web"
	"strings"
	"time"
)

var httpPort string							//http监听port
var pushMsgToken string						//推送消息的token

func init(){
	flag.StringVar(&httpPort,"httpPort",":8099","the port listen for web request")
	flag.StringVar(&pushMsgToken,"pushMsgToken","","the token to authorize business server post message to clients")
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func receivePost(w http.ResponseWriter,r *http.Request){
	room := r.PostFormValue("room")
	postData := r.PostFormValue("jsonData")
	isEnsure := r.PostFormValue("isEnsure")
	token := r.PostFormValue("token")
	if token != pushMsgToken {
		result := web.WebResult{
			Code:400,
			Msg:"token error",
			ServerTime:time.Now().Unix(),
			Data:nil,
		}
		jsonResult,_ := json.Marshal(&result)
		w.Write(jsonResult)
		return
	}
	logrus.Info("receive post msg room------"+room)
	logrus.Info("receive post msg ------"+postData)
	server.GetMsgManager().DispatchMsg(room,postData,isEnsure)
	result := web.WebResult{
		Code:200,
		Msg:"success",
		ServerTime:time.Now().Unix(),
		Data:nil,
	}
	jsonResult,_ := json.Marshal(&result)
	w.Write(jsonResult)
}

//check cmd start params is valid or not
func checkCmdParam() bool{
	flag.Parse()
	if strings.Index(httpPort,":") != 0 {
		fmt.Println("the param httpPort must be liken :8099")
		return false
	}

	if pushMsgToken == "" {
		fmt.Println("must config the pushMsgToken")
		return false
	}
	return true
}

func main() {
	if !checkCmdParam() {
		os.Exit(0)
	}

	wsServer := server.NewWsServer()
	server.GetMsgManager().Run()
	go wsServer.Serve()
	defer wsServer.Close()

	http.HandleFunc("/socket.io/", func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != ""{
			w.Header().Set("Access-Control-Allow-Origin",origin)
		}else{
			w.Header().Set("Access-Control-Allow-Origin","*")
		}
		w.Header().Set("Access-Control-Allow-Credentials","true")
		//删除Origin以避开websocket的域名校验
		r.Header.Del("Origin")
		wsServer.ServeHTTP(w,r)
	})
	http.HandleFunc("/postMsg",receivePost)

	logrus.Info("Serving at localhost "+httpPort)
	logrus.Info(http.ListenAndServe(httpPort, nil))
}
