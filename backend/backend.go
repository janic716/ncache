package backend

import (
	"ncache/backend/nodes"
	"ncache/protocol"
)

type Backend interface {
	Proc(req *protocol.Msg) (*protocol.Msg, error)
	GetNodeIndexByKey([]byte) uint32
	ForwardMsg(uint32, *protocol.Msg) (*protocol.Msg, error)
	GetNodes() []*nodes.Node
	GetConf() interface{}
}
