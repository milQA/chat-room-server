package server

import (
	"fmt"
	"github.com/googollee/go-socket.io"
	"github.com/sirupsen/logrus"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var msgId uint32

type PostMsg struct {
	Id string					`json:"msgId"`			//消息id，用于ws客户端和服务器确认收到该消息
	Room string					`json:"room"`			//房间号
	PostData interface{}		`json:"postData"`		//接口收到的数据，原样转发给ws客户端
	IsEnsure bool				`json:"isEnsure"`		//是否需要保证成功送达
}

type EnsureMsgSendInfo struct {
	msg       *PostMsg
	startTime time.Time					   	//用于判定超时重发
	sendIdMap map[string]socketio.Conn      //key为链接的id，方便收到确认消息后删除链接
	sync.RWMutex
}
//用于离开和加入房间时通过通道同步数据
type ConnRoomChangeInfo struct {
	Room string
	Conn socketio.Conn
}
//用于客户端返回确认接收消息后同步数据
type ConfirmMsg struct {
	MsgId string
	Conn socketio.Conn
}

//产生全局唯一的消息id，考虑到服务可能会重启，所以需要加入时间因子
func getMsgId() string{
	msgId := atomic.AddUint32(&msgId,1)
	return strconv.FormatInt(time.Now().Unix(),10)+":"+strconv.FormatUint(uint64(msgId),10)
}

func newPostMsg(room string,postData string,isEnsure string)*PostMsg{
	msg := &PostMsg{
		Id:getMsgId(),
		Room:room,
		PostData:postData,
	}
	msg.IsEnsure = isEnsure == "1"
	return msg
}

var msgManager *MsgManager

func GetMsgManager() *MsgManager{
	if msgManager != nil{
		return msgManager
	}
	once := sync.Once{}
	once.Do(func() {
		msgManager = &MsgManager{
			msgDispatcher:    NewMsgDispatcher(10,5,5),
			JoinRoomChan:     make(chan *ConnRoomChangeInfo,256),
			LeavaRoomChan:    make(chan *ConnRoomChangeInfo,256),
			LeaveAllRoomChan: make(chan socketio.Conn,100),
			rooms:            sync.Map{},
		}
	})
	return msgManager
}

type MsgRoom struct {
	Name string
	ConnsMap sync.Map		//健名为conn.id,键值为conn
	num int32
}

func (mr *MsgRoom) joinRoom(conn socketio.Conn){
 	if mr.HasConn(conn){
		return
	}
	mr.ConnsMap.Store(conn.ID(),conn)
	atomic.AddInt32(&mr.num,1)
	logrus.Info(fmt.Sprintf("after join room %s has wsclient num %d",mr.Name,mr.num))
}

func (mr *MsgRoom) LeaveRoom(conn socketio.Conn){
	if !mr.HasConn(conn){
		return
	}
	_,ok := mr.ConnsMap.Load(conn.ID())
	//连接在房间内则删除，否则忽略
	if ok {
		mr.ConnsMap.Delete(conn.ID())
		atomic.AddInt32(&mr.num,-1)
	}
	logrus.Info(fmt.Sprintf("after leave room %s has wsclient num %d",mr.Name,mr.num))
}

func (mr *MsgRoom) HasConn(conn socketio.Conn) bool{
	_,ok := mr.ConnsMap.Load(conn.ID())
	//已经在房间内则直接忽略
	return ok
}

type MsgManager struct {
	msgDispatcher    *MsgDispatcher           //消息分发器
	JoinRoomChan     chan *ConnRoomChangeInfo //离开房间的通信通道
	LeavaRoomChan    chan *ConnRoomChangeInfo //进入房间的通信通道
	LeaveAllRoomChan chan socketio.Conn       //用于客户端异常断开等情况退出所有房间
	rooms            sync.Map                 //第一个key是房间号，值为msgRoom结构
	totolConns int32	 					  //总连接数
}

func (s *MsgManager) Run(){
	go s.loopRoomConns()
	go s.msgDispatcher.run()
}
//用户进出房间的操作不会很频繁，暂时不用多协程处理
func (s *MsgManager) loopRoomConns(){
	for {
		select {
			case join := <- s.JoinRoomChan:
				s.doJoinRoom(join)
			case leave := <- s.LeavaRoomChan:
				s.doLeaveRoom(leave)
			case conn := <- s.LeaveAllRoomChan:
				s.doLeaveAllRoom(conn)
		}
	}
}
//客户端加入房间
func (s *MsgManager) doJoinRoom(join *ConnRoomChangeInfo){
	msgRoom,ok := s.rooms.Load(join.Room)
	if ok {
		msgRoom.(*MsgRoom).joinRoom(join.Conn)
	}else{
		newMsgRoom := MsgRoom{
			Name:     join.Room,
			ConnsMap: sync.Map{},
		}
		newMsgRoom.joinRoom(join.Conn)
		s.rooms.Store(join.Room,&newMsgRoom)
	}
}
//客户端离开房间
func (s *MsgManager) doLeaveRoom(leave *ConnRoomChangeInfo){
	msgRoom,ok := s.rooms.Load(leave.Room)
	if ok {
		msgRoom.(*MsgRoom).LeaveRoom(leave.Conn)
	}
}
//客户端离开所有房间
func (s *MsgManager) doLeaveAllRoom(conn socketio.Conn){
	logrus.Info(conn.ID()+" leave all room ")
	s.rooms.Range(func(k,v interface{}) bool{
		v.(*MsgRoom).LeaveRoom(conn)
		return true
	})
}


//检查某一个链接是否在某一个房间内
func (s *MsgManager) RoomHasConn(room string, conn socketio.Conn) bool{
	msgRoom,ok := s.rooms.Load(room)
	if ok {
		return msgRoom.(*MsgRoom).HasConn(conn)
	}
	return false
}

func (s *MsgManager)DispatchMsg(room,postData,isEnsure string){
	msg := newPostMsg(room,postData,isEnsure)
	s.msgDispatcher.dispatchMsg(msg)
}

func (s *MsgManager)JoinRoom(room string,conn socketio.Conn){
	roomChange := &ConnRoomChangeInfo{
		room,
		conn,
	}
	s.JoinRoomChan <- roomChange
}

func (s *MsgManager)LeaveRoom(room string,conn socketio.Conn){
	roomChange := &ConnRoomChangeInfo{
		room,
		conn,
	}
	s.LeavaRoomChan <- roomChange
}

func (s *MsgManager)LeaveAllRoom(conn socketio.Conn){
	s.LeaveAllRoomChan <- conn
}

func (s *MsgManager)ConfirmMsg(conn socketio.Conn,msgId string){
	msgConf := ConfirmMsg{
		msgId,
		conn,
	}
	s.msgDispatcher.confirmMsg(&msgConf)
}



