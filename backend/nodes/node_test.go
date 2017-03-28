package nodes

import (
	"fmt"
	"ncache/config"
	"ncache/protocol"
	"ncache/utils"
	"testing"
)

func newNode() *Node {
	masterAddr := "jelly02:26379"
	slave1Addr := "jelly02:26380"
	slave2Addr := "jelly02:26381"
	masterDbConf := &config.DbConf{
		Addr:         masterAddr,
		InitConnNum:  20,
		MaxConnNum:   50,
		ConnTimeout:  1000,
		ReadTimeout:  1000,
		WriteTimeout: 1000,
	}
	slave1DbConf := &config.DbConf{
		Addr:         slave1Addr,
		InitConnNum:  20,
		MaxConnNum:   50,
		ConnTimeout:  1000,
		ReadTimeout:  1000,
		WriteTimeout: 1000,
	}
	slave2DbConf := &config.DbConf{
		Addr:         slave2Addr,
		InitConnNum:  20,
		MaxConnNum:   50,
		ConnTimeout:  1000,
		ReadTimeout:  1000,
		WriteTimeout: 1000,
	}
	var slaves []*config.DbConf
	slaves = append(slaves, slave1DbConf, slave2DbConf)
	nodeConf := &config.NodeConf{
		Mode:   ModeSingle,
		Master: masterDbConf,
		Slaves: slaves,
	}
	if node, err := NewNode(nodeConf); err == nil {
		return node
	} else {
		utils.AssertMustNoError(err)
	}
	return nil
}

func TestNode_RelayMsg(t *testing.T) {
	node := newNode()
	var msg *protocol.Msg
	var err error
	msg, err = node.RelayMsg(protocol.NewCmdMsg("set a 100"))
	fmt.Println(msg.GetStr())
	msg, err = node.RelayMsg(protocol.NewCmdMsg("get a"))
	fmt.Println(msg.GetStr())
	msg, err = node.RelayMsg(protocol.NewCmdMsg("del a"))
	fmt.Println(msg.GetStr())
	msg, err = node.RelayMsg(protocol.NewCmdMsg("del a"))
	fmt.Println(msg.GetStr())
	fmt.Println(err)
}
