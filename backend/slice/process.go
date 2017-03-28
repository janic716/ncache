package slice

import (
	"ncache/protocol"
)

func (s *Slice) ForwardMsg(index uint32, msg *protocol.Msg) (msgAck *protocol.Msg, err error) {
	node := s.GetNodeByIndex(index)
	return node.RelayMsg(msg)
}
