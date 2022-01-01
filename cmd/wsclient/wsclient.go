package main

import (
	"encoding/json"
	"fmt"
	"github.com/zhouhui8915/go-socket.io-client"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

type PostMsg struct {
	Id string					`json:"msgId"`			//消息id，全局唯一，质量消息时，作为消息回执
	Room string					`json:"room"`			//房间号
	PostData interface{}		`json:"postData"`		//接口收到的数据，原样转发给ws客户端
	IsEnsure bool				`json:"isEnsure"`		//是否为质量消息（一定时间内客户端未发送消息回执则再次发送）
	EnsureSeconds int			`json:'ensureSeconds'`	//质量消息重发时限，秒
}

func main() {
	opts := &socketio_client.Options{
		//Transport:"polling",
		Transport:"websocket",
		Query:     make(map[string]string),
	}
	opts.Query["user"] = "user"
	opts.Query["pwd"] = "pass"
	uri := "http://127.0.0.1:8099"

	stressTest(uri,opts,1)
}

func stressTest(uri string,opts *socketio_client.Options,nums int){
	stopChan := make(chan int,1024)
	receiveMsgChan := make(chan int,1024)

	for i:=0;i< nums;i++  {
		go connnetServer(uri,opts,i,stopChan,receiveMsgChan)
		time.Sleep(time.Millisecond*50)
	}

	var i int32 = 0
	var j int32 = 0

	for {
		select {
		case val := <-stopChan:
			fmt.Println(strconv.Itoa(val) + " disconect")
			atomic.AddInt32(&i, 1)
			fmt.Println("disconnect num " + strconv.Itoa(int(i)))
		case index := <-receiveMsgChan:
			fmt.Println(strconv.Itoa(index) + " receive msg")
			atomic.AddInt32(&j, 1)
			fmt.Println("receive num " + strconv.Itoa(int(j)))
		}
	}
}

func connnetServer(uri string,opts *socketio_client.Options,i int,stopChan chan int,receiveChan chan int){
	client, err := socketio_client.NewClient(uri, opts)
	if err != nil {
		log.Println("NewClient error:%v\n", err)
		return
	}
	client.Emit("joinRoom","live")
	client.On("error", func() {
		log.Println("on error\n")
	})
	client.On("connection", func() {
		log.Println("on connect\n")
	})
	client.On("commonMessage", func(msg string) {
		//log.Printf("on commonMessage:%v\n", msg)
		receiveChan <- i
	})
	client.On("ensureMessage", func(msg string) {
		log.Println("on ensureMessage:%v\n", msg)
		postMsg := PostMsg{}
		json.Unmarshal([]byte(msg), &postMsg)
		client.Emit("confirmMessage", postMsg.Id)
	})
	client.On("disconnection", func() {
		stopChan <- i
		//log.Printf("on disconnect\n")
		//os.Exit(0)
	})
}