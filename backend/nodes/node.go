package nodes

import (
	"fmt"
	"ncache/config"
	"ncache/filter"
	"ncache/protocol"
	"sync"
)

//todo : redis 节点, 需封装读写操作
const (
	ModeSingle = 0x00
	ModeMasterSlave = 0x01
	ModeHashSlice = 0x02
	ModeMixed = 0x03
)

var (
	desc_init_node_err = "init node failed"
)

type Node struct {
	conf           *config.NodeConf

	mux            sync.RWMutex
	mode           byte
	master         *Db
	slaves         []*Db
	weights        []int
	lastSalveIndex int32
	balance        balanceFunc
	getDbByMsgFn   func(msg *protocol.Msg) *Db
	// The following two items Used by slice
	weight         int
	name           string
}

func NewNode(conf *config.NodeConf) (*Node, error) {
	node := &Node{
		conf:   conf,
		weight: conf.Weight,
		name:   conf.Name,
	}
	node.mode = conf.Mode
	var (
		err error
		db  *Db
	)
	if node.master, err = NewDb(conf.Master); err != nil {
		return nil, fmt.Errorf("%s, addr: %s", desc_init_node_err, conf.Master.Addr)
	}
	slaveLen := len(conf.Slaves)
	if slaveLen > 0 {
		node.slaves = make([]*Db, 0, slaveLen)
	}
	if node.mode == ModeMasterSlave {
		for i := 0; i < slaveLen; i++ {
			if db, err = NewDb(conf.Slaves[i]); err == nil {
				node.slaves = append(node.slaves, db)
			}
		}
	}
	node.balance = normalBalance

	node.getDbByMsgFn = fnGetDbByMsg(node)
	return node, nil
}

//func (this *Node) dbsHealthCheck() {
//	var err error
//	if this.master == nil {
//		this.master, _ = NewDb(this.conf.Master)
//	}
//	if this.master != nil {
//		this.master.healthCheck(3, 100)
//	}
//	slaveLen := this.SlaveLen()
//	if slaveLen > 0 {
//		for i := 0; i < slaveLen; i++ {
//			this.slaves[i].healthCheck(2, 100)
//		}
//	}
//}

func (this *Node) SlaveLen() int {
	this.mux.RLock()
	res := len(this.slaves)
	this.mux.RUnlock()
	return res
}

func fnGetDbByMsg(node *Node) func(msg *protocol.Msg) *Db {
	defaultFunc := func(msg *protocol.Msg) *Db {
		return node.master
	}
	switch node.mode {
	case ModeSingle:
		return defaultFunc
	case ModeMasterSlave:
		return func(msg *protocol.Msg) *Db {
			if filter.IsWriteCmdMsg(msg) || node.SlaveLen() == 0 {
				return node.master
			} else {
				return node.getSlaveDbBalance()
			}
		}
	case ModeHashSlice, ModeMixed: //todo: 待支持
		return defaultFunc
	}
	return nil
}

func (this *Node) getSlaveDbBalance() *Db {
	index := this.balance(this)
	return this.slaves[index]
}

func (this *Node) RelayMsg(msg *protocol.Msg) (*protocol.Msg, error) {
	db := this.getDbByMsgFn(msg)
	return db.ProcCmdMsg(msg)
}

// 实现简单版, 串行执行
func (this *Node) RelayMultiMsg(msgList []*protocol.Msg) (msg []*protocol.Msg, err error) {
	if len(msgList) == 0 {
		return nil, nil
	}
	var db *Db
	switch this.mode {
	case ModeSingle:
		db = this.getDbByMsgFn(msgList[0])
	case ModeMasterSlave:
		for _, m := range msgList {
			if filter.IsWriteCmdMsg(m) {
				db = this.master
				break
			} else if db != nil {
				db = this.getSlaveDbBalance()
			}
		}
	}
	if db != nil {
		return db.ProcMultiCmdMsg(msgList)
	}
	return
}

func (this *Node) GetMasterAddress() string {
	return this.conf.Master.Addr
}

func (this *Node) GetWeight() int {
	return this.weight
}

func (this *Node) GetName() string {
	return this.name
}

func (this *Node) GetDb(msg *protocol.Msg) *Db {
	return this.getDbByMsgFn(msg)
}
