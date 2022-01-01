package server

import (
	"github.com/googollee/go-socket.io"
	"github.com/sirupsen/logrus"
	"strconv"
	"sync/atomic"
)

var wsServer *socketio.Server

func NewWsServer()*socketio.Server{
	if wsServer != nil{
		return wsServer
	}
	newServer := socketio.NewServer(nil)
	wsServer = newServer
	registerEnvet()
	return wsServer
}

func registerEnvet(){
	wsServer.OnConnect("/", func(conn socketio.Conn) error {
		logrus.Info(conn.ID()+" "+conn.RemoteAddr().String()+" connect ")
		atomic.AddInt32(&GetMsgManager().totolConns,1)
		logrus.Info("after join total conn num "+strconv.Itoa(int(GetMsgManager().totolConns)))
		return nil
	})

	wsServer.OnError("/", func(conn socketio.Conn, e error) {
		//on error 时不能调用任何conn相关的方法，因为此时无法保证底层实现conn接口的对象是否已经被释放引发panic
		logrus.Info("meet error "+e.Error())
	})

	//加入房间事件
	wsServer.OnEvent("/", "joinRoom", func(conn socketio.Conn, room string) {
		logrus.Info(conn.ID()+" "+conn.RemoteAddr().String()+" joinRoom "+room)
		GetMsgManager().JoinRoom(room,conn)
		conn.Join(room)
	})

	//离开房间
	wsServer.OnEvent("/", "leaveRoom", func(conn socketio.Conn, room string) {
		logrus.Info(conn.ID()+" "+conn.RemoteAddr().String()+" leaveRoom "+room)
		GetMsgManager().LeaveRoom(room,conn)
		conn.Leave(room)
	})

	//离开所有房间事件
	wsServer.OnEvent("/", "leaveAllRoom", func(conn socketio.Conn) {
		logrus.Info(conn.ID()+" "+conn.RemoteAddr().String()+" leaveAllRoom")
		GetMsgManager().LeaveAllRoom(conn)
		conn.LeaveAll()
	})

	//确认消息事件
	wsServer.OnEvent("/", "confirmMessage", func(conn socketio.Conn,msgId string){
		logrus.Info(conn.ID()+" "+conn.RemoteAddr().String()+" confirmMessage "+msgId)
		GetMsgManager().ConfirmMsg(conn,msgId)
	})

	wsServer.OnDisconnect("/", func(conn socketio.Conn, s string) {
		logrus.Info(conn.ID()+" "+conn.RemoteAddr().String()+" disconnect "+s)
		GetMsgManager().LeaveAllRoom(conn)
		conn.LeaveAll()
		atomic.AddInt32(&GetMsgManager().totolConns,-1)
		logrus.Info("after leave total conn num "+strconv.Itoa(int(GetMsgManager().totolConns)))
	})
}