package cluster

import (
	"bytes"
	"fmt"
	"ncache/backend/nodes"
	"ncache/protocol"
)

type ResMap map[*nodes.Node]*protocol.Msg

func (c *Cluster) ForwardMsg(index uint32, msg *protocol.Msg) (msgAck *protocol.Msg, err error) {
	var (
		node *nodes.Node
		ok   bool
	)
	if node, ok = c.slots.getSlot(uint16(index)); !ok {
		return nil, fmt.Errorf("slot %d not specified", index)
	}
	return c.forwardMsg(node, msg, false, 0)
}

func (c *Cluster) forwardMsg(node *nodes.Node, msg *protocol.Msg, isAsking bool, redirectTime int) (msgAck *protocol.Msg, err error) {
	if redirectTime > c.redirectTime {
		return nil, fmt.Errorf("Exceed max redirect time: %d", c.redirectTime)
	}
	db := node.GetDb(msg)
	msgAck, err = forwardMsgToDb(msg, db, isAsking)
	if err != nil {
		return nil, err
	}
	if msgAck.IsError() {
		bytesValue, _ := msgAck.GetValueBytes()
		var msgAckBytesValueSplit [][]byte = bytes.Fields(bytesValue)
		if len(msgAckBytesValueSplit) != 3 {
			return
		}
		switch {
		case bytes.EqualFold(msgAckBytesValueSplit[0], []byte("MOVED")):
			addr := string(msgAckBytesValueSplit[2])
			movedNode, err := c.GetNodeByMasterAddr(addr)
			if err != nil {
				return nil, err
			}
			return c.forwardMsg(movedNode, msg, false, redirectTime+1)
		case bytes.EqualFold(msgAckBytesValueSplit[0], []byte("ASK")):
			addr := string(msgAckBytesValueSplit[2])
			movedNode, err := c.GetNodeByMasterAddr(addr)
			if err != nil {
				return nil, err
			}
			return c.forwardMsg(movedNode, msg, true, redirectTime+1)
		}
	}
	return
}

func forwardMsgToDb(msg *protocol.Msg, db *nodes.Db, isAsking bool) (ack *protocol.Msg, err error) {
	conn, err := db.GetConnect()
	if err != nil {
		return nil, err
	}
	defer db.PutConn(conn)
	if !db.IsMaster() {
		if err = conn.SendReadOnly(); err != nil {
			return nil, err
		}
	}
	if isAsking {
		if err = conn.SendAsking(); err != nil {
			return nil, err
		}
	}
	if ack, err = conn.HandleMsg(msg); err != nil {
		return nil, err
	}
	return ack, nil
}
