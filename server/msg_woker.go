package server

import (
	"encoding/json"
	"github.com/googollee/go-socket.io"
	"github.com/sirupsen/logrus"
	"strconv"
	"sync"
	"time"
)
//发送消息的worker
type MsgWorker struct {
	id int
	msgChan chan *PostMsg								//用于接收待发送的消息
	msgChanPool chan chan *PostMsg						//用于接收空闲的可接收消息的通道
	disPatcher *MsgDispatcher
}

//即来即走，完成工作报备
func (w *MsgWorker)work(){
	logrus.Info("msg worker "+strconv.Itoa(w.id)+" start work")
	for {
		//每次空闲时，woker将自己接受消息的通道注册到工作池
		w.msgChanPool <- w.msgChan
		select {
			case msg := <- w.msgChan:
				//需要确认的消息
				if msg.IsEnsure {
					w.disPatcher.sendEnsureMsg(msg)
				}else{
					w.disPatcher.sendCommonMsg(msg)
				}
		}
	}
}
//接收客户端消息回执的worker
type ConfirmMsgWorker struct {
	id                 int
	confirmMsgChan     chan *ConfirmMsg						//用于接收ws客户端发送的确认消息回执
	ConfirmMsgChanPool chan chan *ConfirmMsg
	disPatcher         *MsgDispatcher
}

//即来即走，完成工作报备
func (w *ConfirmMsgWorker)work(){
	logrus.Info("confirm msg worker "+strconv.Itoa(w.id)+" start work")
	for {
		//每次空闲时，woker将自己接受消息的通道注册到工作池
		w.ConfirmMsgChanPool <- w.confirmMsgChan
		select {
			case msg := <- w.confirmMsgChan:
				w.disPatcher.doConfirmMsg(msg)
		}
	}
}

//接收客户端消息回执的worker
type ReSendMsgWorker struct {
	id                 int
	ReSendMsgChan      chan []string						//用于接收ws客户端发送的确认消息回执
	ReSendChanPool 	   chan chan []string
	disPatcher         *MsgDispatcher
}

//即来即走，完成工作报备
func (w *ReSendMsgWorker)work(){
	logrus.Info("resend msg worker "+strconv.Itoa(w.id)+" start work")
	for {
		//每次空闲时，woker将自己接受消息的通道注册到工作池
		w.ReSendChanPool <- w.ReSendMsgChan
		select {
			case msgIdArr := <- w.ReSendMsgChan:
				for _,msgId := range msgIdArr{
					w.disPatcher.reSendEnsureMsg(msgId)
				}
		}
	}
}

type MsgDispatcher struct {
	msgChan     chan *PostMsg      					//带缓冲的通道，用于接收外部传进来的msg
	msgChanPool chan chan *PostMsg 					//接收准备好的worker的msg通道
	msgWorkers  []*MsgWorker       					//worker数组，用于控制woker

	ConfirmMsgChan   chan *ConfirmMsg              //客户端消息确认同步通道
	ConfirmMsgChanPool chan chan *ConfirmMsg
	confirmWorkers []*ConfirmMsgWorker

	reSendMsgChan     chan []string              //客户端消息确认同步通道
	reSendMsgChanPool chan chan []string
	reSendWorkers     []*ReSendMsgWorker

	EnsureMsgSendMap map[string]*EnsureMsgSendInfo //存放消息的id和客户端id的映射,以及该发送给该客户端发送消息的时间
	msgSendLock      sync.RWMutex                  //用于控制确认送达消息读写的锁
	popEnsureMsgChan chan []string					//从延时队列取待确认消息的msgId数组
	ringQueue *RingQueue
}

func (p *MsgDispatcher)run(){
	//启动msg worker
	for _,worker := range p.msgWorkers {
		go worker.work()
	}
	//启动confirm worker
	for _,confirmWorker := range p.confirmWorkers{
		go confirmWorker.work()
	}

	//启动resend confirm worker
	for _,worker := range p.reSendWorkers{
		go worker.work()
	}

	//启动wokerPool
	go p.loopMsg()
	go p.ensureMsg()
	go p.ringQueue.Run()
}
//接收外部传进来的消息
func (p *MsgDispatcher)dispatchMsg(msg *PostMsg){
	p.msgChan <- msg
}

func (p *MsgDispatcher)confirmMsg(msg *ConfirmMsg){
	p.ConfirmMsgChan <- msg
}

func (p *MsgDispatcher)loopMsg(){
	logrus.Info("loopMsg start ")
	for{
		select {
			//从消息通道里取消息分给准备好的woker
			case msg := <- p.msgChan:
				workerChan := <- p.msgChanPool
				workerChan <- msg
			case confirmMsg := <- p.ConfirmMsgChan:
				confirmWorker := <- p.ConfirmMsgChanPool
				confirmWorker <- confirmMsg
		}
	}
}

//发送普通消息
func (p *MsgDispatcher)sendCommonMsg(msg *PostMsg){
	jsonMsg,err := json.Marshal(msg)
	if err != nil{
		logrus.Error("Marshal msg error with msg data :"+msg.PostData.(string))
		return
	}
	logrus.Info("send common message")
	wsServer.BroadcastToRoom("/",msg.Room,"commonMessage",string(jsonMsg))
}

//发送需要确认的消息
func (p *MsgDispatcher)sendEnsureMsg(msg *PostMsg){
	emsi := EnsureMsgSendInfo{
		msg:       msg,
		startTime: time.Now(),
		sendIdMap: make(map[string]socketio.Conn),
	}
	p.msgSendLock.Lock()
	p.EnsureMsgSendMap[msg.Id] = &emsi
	p.msgSendLock.Unlock()
	logrus.Info("send ensure message")
	emsi.Lock()
	defer emsi.Unlock()
	jsonMsg,err := json.Marshal(msg)
	if err != nil{
		logrus.Error("Marshal msg error with msg data :"+msg.PostData.(string))
		return
	}

	sendNum := 0
	wsServer.ForEach("/",msg.Room, func(conn socketio.Conn) {
		sendNum++
		//将conn放入发送map中
		emsi.sendIdMap[conn.ID()] = conn
		conn.Emit("ensureMessage",string(jsonMsg))
	})
	//有客户端接收到该信息，则加入延时任务队列，否则不加入
	if sendNum > 0{
		//将重发任务加入延时任务队列
		p.ringQueue.Add(10,msg.Id)
	}
}
//给未确认收到消息的客户端发送消息
func (p *MsgDispatcher)reSendEnsureMsg(msgId string){
	p.msgSendLock.RLock()
	ensureSendInfo,ok := p.EnsureMsgSendMap[msgId]
	p.msgSendLock.RUnlock()
	if ok{
		//如果该消息id下客户端都已确认收到消息，则删除该msgId
		if len(ensureSendInfo.sendIdMap) == 0 {
			p.msgSendLock.Lock()
			delete(p.EnsureMsgSendMap, msgId)
			defer p.msgSendLock.Unlock()
			return
		}
		sendNum := 0
		for _,v := range ensureSendInfo.sendIdMap{
			if msgManager.RoomHasConn(ensureSendInfo.msg.Room,v){
				jsonMsg,err := json.Marshal(ensureSendInfo.msg)
				if err != nil{
					logrus.Error("Marshal msg error with msg data :"+ensureSendInfo.msg.PostData.(string))
					continue
				}
				sendNum++
				v.Emit("ensureMessage",string(jsonMsg))
			}else{
				ensureSendInfo.Lock()
				delete(ensureSendInfo.sendIdMap,v.ID())
				ensureSendInfo.Unlock()
			}
		}
		//有用户接收到该消息才加入延时任务队列
		if sendNum > 0{
			//将msgId加入延时队列
			p.ringQueue.Add(10,msgId)
		}
	}
}

//处理客户端消息确认回执
func (p *MsgDispatcher) doConfirmMsg(msg *ConfirmMsg) {
	p.msgSendLock.Lock()
	if mmap,ok := p.EnsureMsgSendMap[msg.MsgId];ok{
		mmap.Lock()
		delete(mmap.sendIdMap,msg.Conn.ID())
		mmap.Unlock()
	}
	p.msgSendLock.Unlock()
}

//消息发送后确保发送成功
func (p *MsgDispatcher) ensureMsg(){
	logrus.Info("ensureMsg start")
	for {
		select {
			case msgIdStr := <- p.popEnsureMsgChan:
				select {
					case resend := <-p.reSendMsgChanPool:
						resend <- msgIdStr
				}
		}
	}
}

//创建消息分发器，并初始化消息工作者
func NewMsgDispatcher(msgWorkerNum,confirmMsgWorkerNum, reSendWorkerNum int)* MsgDispatcher{
	dispather := MsgDispatcher{
		msgChan:            make(chan *PostMsg,1024),
		msgChanPool:        make(chan chan *PostMsg, msgWorkerNum),
		msgWorkers:         []*MsgWorker{},

		ConfirmMsgChan:     make(chan *ConfirmMsg,1024),
		ConfirmMsgChanPool: make(chan chan *ConfirmMsg, msgWorkerNum),
		confirmWorkers:		[]*ConfirmMsgWorker{},

		reSendMsgChan:		make(chan []string,100),
		reSendMsgChanPool:  make(chan chan []string),
		reSendWorkers:		[]*ReSendMsgWorker{},

		msgSendLock:        sync.RWMutex{},
		EnsureMsgSendMap: make(map[string]*EnsureMsgSendInfo),
		popEnsureMsgChan: make(chan []string,50),
	}
	dispather.ringQueue = NewRingQueue(60,dispather.popEnsureMsgChan)
	//初始化发送消息工作者
	for i:=0;i< msgWorkerNum;i++{
		newWorker := MsgWorker{
			id:i,
			msgChan:make(chan *PostMsg,1),
			msgChanPool:dispather.msgChanPool,
			disPatcher:&dispather,
		}
		dispather.msgWorkers = append(dispather.msgWorkers,&newWorker)
	}
	//初始化确认消息回执工作者
	for i:=0;i< confirmMsgWorkerNum;i++{
		confirmWorker := ConfirmMsgWorker{
			id:                 i,
			confirmMsgChan:     make(chan *ConfirmMsg,1),
			ConfirmMsgChanPool: dispather.ConfirmMsgChanPool,
			disPatcher:         &dispather,
		}
		dispather.confirmWorkers = append(dispather.confirmWorkers,&confirmWorker)
	}

	for i:=0;i< reSendWorkerNum;i++{
		confirmWorker := ReSendMsgWorker{
			id:                 i,
			ReSendMsgChan:     make(chan []string,1),
			ReSendChanPool: 	dispather.reSendMsgChanPool,
			disPatcher:         &dispather,
		}
		dispather.reSendWorkers = append(dispather.reSendWorkers,&confirmWorker)
	}

	return &dispather
}