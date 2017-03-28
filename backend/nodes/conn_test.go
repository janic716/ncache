package nodes

import (
	"fmt"
	"ncache/config"
	"ncache/protocol"
	"ncache/utils"
	"testing"
)

func TestConn_Connect(t *testing.T) {
	addr := "127.0.0.1:6379"
	conn := newConn(addr)
	err := protocol.NewCmdMsg("Ping").WriteMsg(conn)
	if err != nil {
		utils.AssertMustNoError(err)
	}
	if msg, err := protocol.NewMsgFromReader(conn); err == nil && !msg.IsError() {
		if str, err := msg.GetStr(); err == nil {
			utils.AssertMust(str == "PONG")
		}
	} else {
		utils.AssertMustNoError(err)
	}
}

func TestConn_ReadAndWrite(t *testing.T) {
	addr := "127.0.0.1:6379"
	conn := newConn(addr)
	err := protocol.NewCmdMsg("set ncache \"hello world\"").WriteMsg(conn)
	if err != nil {
		utils.AssertMustNoError(err)
	}
	if msg, err := protocol.NewMsgFromReader(conn); err == nil && protocol.IsOkMsg(msg) {
		err = protocol.NewCmdMsg("get ncache ").WriteMsg(conn)
		if msg, err = protocol.NewMsgFromReader(conn); err == nil {
			if str, err := msg.GetStr(); err == nil {
				utils.AssertMust(str == "hello world")
				protocol.NewCmdMsg("del ncache").WriteMsg(conn)
			} else {
				utils.AssertMustNoError(err)
			}
		} else {
			utils.AssertMustNoError(err)
		}
	} else {
		utils.AssertMustNoError(err)
	}
	conn.Close()
	msg, err := protocol.NewMsgFromReader(conn)
	fmt.Println(msg, err)
}

func TestConn_Write(t *testing.T) {

}

func newConn(addr string) (conn *Conn) {
	dbConf := &config.DbConf{
		Addr:         addr,
		ConnTimeout:  1000,
		ReadTimeout:  1000,
		WriteTimeout: 1000,
	}
	var err error
	if conn, err = NewConn(addr, protocol.Ping, dbConf); err != nil {
		utils.AssertMustNoError(err)
	}
	return
}
