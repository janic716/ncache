package nodes

import (
	"fmt"
	"ncache/config"
	"ncache/protocol"
	"ncache/utils"
	"testing"
)

func TestNewNode(t *testing.T) {
	addr := "127.0.0.1:6379"
	db := newDb(addr)
	msg, _ := db.ProcCmdMsg(protocol.NewCmdMsg("ping"))
	fmt.Println(msg.GetStr())
}

func newDb(addr string) *Db {
	dbConf := &config.DbConf{
		Addr:         addr,
		InitConnNum:  20,
		MaxConnNum:   50,
		ConnTimeout:  1000,
		ReadTimeout:  1000,
		WriteTimeout: 1000,
	}
	if db, err := NewDb(dbConf); err == nil {
		return db
	} else {
		utils.AssertMustNoError(err)
	}
	return nil
}

func TestDb_CloseDb(t *testing.T) {

}

func TestDb_ProcCmdMsg(t *testing.T) {

}

func TestDb_ProcMultiCmdMsg(t *testing.T) {

}
