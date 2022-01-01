package server

import (
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

type RingQueue struct {
	slotNum       int           //时间轮槽数
	ringQueueList []sync.Map    //key为msgId,值为cycleNum int值
	currIndex     int           //当前指针
	popChan       chan []string //往外输出msgId的通道
}

//将新任务加入环形队列
func (rq *RingQueue) Add(sec int,msgId string){
	cycleNum := sec/rq.slotNum
	index := rq.getIndex(sec)
	itemList := &rq.ringQueueList[index]			//sync.Map为结构体，此处需要使用其指针
	itemList.Store(msgId,cycleNum)
}
//获取新入队队列存入ringQueue的索引
func (rq *RingQueue)getIndex(sec int) int{
	//索引只有一个gorutine变动，可以不使用锁了
	index := rq.currIndex + (sec % rq.slotNum)
	//当前索引位置加上距离当前索引的位置不大于总长度，则直接返回
	if (index <  rq.slotNum){
		return index
	}

	return index-rq.slotNum
}
//启动并开启定时器，一秒钟处理一次消息
func (rq *RingQueue)Run(){
	logrus.Info("ring queue start")
	ticker := time.NewTicker(time.Second)
	for{
		select {
			case _  = <- ticker.C:
				list := &(rq.ringQueueList[rq.currIndex])
				msgIdArr := []string{}
				list.Range(func(key, value interface{}) bool {
					if value.(int) == 0{
						msgIdArr = append(msgIdArr,key.(string))
						list.Delete(key)
					}else{
						cycleNum := value.(int)
						cycleNum--
						list.Store(key,cycleNum)
					}
					return true
				})
				if rq.currIndex < rq.slotNum-1{
					rq.currIndex++
				}else{
					rq.currIndex = 0
				}
				if len(msgIdArr) > 0{
					rq.popChan <- msgIdArr
				}
		}
	}

}

func NewRingQueue(queueLength int,popChan chan []string)*RingQueue{
	return &RingQueue{
		slotNum:       queueLength,
		currIndex:     0,
		popChan:       popChan,
		ringQueueList: make([]sync.Map,queueLength),
	}
}

