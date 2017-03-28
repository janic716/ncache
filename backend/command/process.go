package command

import (
	"github.com/janic716/golib/log"
	"ncache/backend"
	"ncache/protocol"
	"sync"
)

const (
	GoroutineNums = 6
	CriticaNum    = 6
)

func KeyProc(be backend.Backend, msg *protocol.Msg) (msgAck *protocol.Msg, err error) {
	var (
		key   []byte
		index uint32
	)
	key, _ = msg.GetArray()[1].GetValueBytes()
	index = be.GetNodeIndexByKey(key)
	msgAck, err = be.ForwardMsg(index, msg)
	return
}

type goRet struct {
	s   *protocol.Msg
	err error
}

func MsetProc(be backend.Backend, msg *protocol.Msg) (msgAck *protocol.Msg, err error) {
	arr := msg.GetArray()
	var (
		groupedMsg map[uint32]*protocol.Msg = make(map[uint32]*protocol.Msg)
		ok         bool
		tempMsg    *protocol.Msg
		index      uint32
		length     int = len(arr)
	)

	for i := 1; i < length; i += 2 {
		key, _ := arr[i].GetValueBytes()
		index = be.GetNodeIndexByKey(key)
		if tempMsg, ok = groupedMsg[index]; !ok {
			tempMsg = protocol.NewArrayMsg(nil).AppendMsg(arr[0])
		}
		groupedMsg[index] = tempMsg.AppendMsg(arr[i]).AppendMsg(arr[i+1])
	}
	// todo ? 考虑mset时对错误的处理？ 忽略错误还是返回错误？
	if len(groupedMsg) < 6 {
		for index, tempMsg = range groupedMsg {
			if msgAck, err = be.ForwardMsg(index, tempMsg); err != nil {
				return nil, err
			}
		}
		return msgAck, nil
	} else {
		parts := make([]map[uint32]*protocol.Msg, GoroutineNums)
		for j := range parts {
			parts[j] = make(map[uint32]*protocol.Msg)
		}
		i := 0
		for index, msg := range groupedMsg {
			parts[i%GoroutineNums][index] = msg
			i++
		}

		f := func(msgMap map[uint32]*protocol.Msg, wg *sync.WaitGroup, ch chan<- goRet) {
			defer wg.Done()
			for index, tempMsg := range msgMap {
				if msgAck, err = be.ForwardMsg(index, tempMsg); err != nil {
					ch <- goRet{nil, err}
					return
				}
			}
			ch <- goRet{nil, nil}
		}
		wg := &sync.WaitGroup{}
		wg.Add(GoroutineNums)
		ch := make(chan goRet, GoroutineNums)
		for i := 0; i < GoroutineNums; i++ {
			go f(parts[i], wg, ch)
		}
		wg.Wait()
		close(ch)
		for c := range ch {
			if c.err != nil {
				return nil, err
			}
		}
		msgAck = protocol.NewSimpleStringMsg("OK")
		return msgAck, nil
	}
}

func MgetProc(be backend.Backend, msg *protocol.Msg) (msgAck *protocol.Msg, err error) {
	arr := msg.GetArray()
	var (
		groupedMsg  map[uint32]*protocol.Msg = make(map[uint32]*protocol.Msg)
		ok          bool
		tempMsg     *protocol.Msg
		keyMsg      *protocol.Msg
		valueMsg    *protocol.Msg
		index       uint32
		length      int = len(arr)
		msgAckSplit *protocol.Msg
		i           int
	)

	for i = 1; i < length; i++ {
		key, _ := arr[i].GetValueBytes()
		index = be.GetNodeIndexByKey(key)
		if tempMsg, ok = groupedMsg[index]; !ok {
			tempMsg = protocol.NewArrayMsg(nil).AppendMsg(arr[0])
		}
		groupedMsg[index] = tempMsg.AppendMsg(arr[i])
	}

	respMap := make(map[*protocol.Msg]*protocol.Msg, length)
	if len(groupedMsg) < CriticaNum {
		for index, tempMsg = range groupedMsg {
			if msgAckSplit, err = be.ForwardMsg(index, tempMsg); err != nil {
				return nil, err
			}
			if !msgAckSplit.IsArray() {
				log.Warningf("Not the expected results")
				continue
			}
			if msgAckSplit.GetArrayLen() != tempMsg.GetArrayLen()-1 {
				log.Warningf("Not the expected results")
				continue
			}
			for i, keyMsg = range tempMsg.GetArray()[1:] {
				respMap[keyMsg] = msgAckSplit.GetArray()[i]
			}
		}
	} else {
		var tmux sync.Mutex
		parts := make([]map[uint32]*protocol.Msg, GoroutineNums)
		for j := range parts {
			parts[j] = make(map[uint32]*protocol.Msg)
		}
		for index, msg := range groupedMsg {
			parts[i%GoroutineNums][index] = msg
			i++
		}
		f := func(mmap map[uint32]*protocol.Msg, wg *sync.WaitGroup, ch chan<- goRet) {
			defer wg.Done()
			var ack *protocol.Msg
			for index, msg := range mmap {
				if ack, err = be.ForwardMsg(index, msg); err != nil {
					ch <- goRet{nil, err}
					return
				}
				if !ack.IsArray() || ack.GetArrayLen() != msg.GetArrayLen()-1 {
					log.Warningf("Not the expected results")
					continue
				}
				for i, keyMsg := range msg.GetArray()[1:] {
					tmux.Lock()
					respMap[keyMsg] = ack.GetArray()[i]
					tmux.Unlock()
				}
			}
			ch <- goRet{nil, nil}
		}
		wg := &sync.WaitGroup{}
		wg.Add(GoroutineNums)
		ch := make(chan goRet, GoroutineNums)
		for i := 0; i < GoroutineNums; i++ {
			go f(parts[i], wg, ch)
		}
		wg.Wait()
		close(ch)
		for c := range ch {
			if c.err != nil {
				return nil, err
			}
		}
	}

	msgAck = protocol.NewArrayMsg(nil)
	for _, keyMsg = range arr[1:] {
		if valueMsg, ok = respMap[keyMsg]; ok {
			msgAck.AppendMsg(valueMsg)
		} else {
			msgAck.AppendMsg(protocol.NullBulkString)
		}
	}
	return
}

func procDelOrExists(be backend.Backend, msg *protocol.Msg) (msgAck *protocol.Msg, err error) {
	arr := msg.GetArray()
	var (
		groupedMsg  map[uint32]*protocol.Msg = make(map[uint32]*protocol.Msg)
		ok          bool
		tempMsg     *protocol.Msg
		index       uint32
		length      int = len(arr)
		msgAckSplit *protocol.Msg
		i           int
	)
	for i = 1; i < length; i++ {
		key, _ := arr[i].GetValueBytes()
		index = be.GetNodeIndexByKey(key)
		if tempMsg, ok = groupedMsg[index]; !ok {
			tempMsg = protocol.NewArrayMsg(nil).AppendMsg(arr[0])
		}
		groupedMsg[index] = tempMsg.AppendMsg(arr[i])
	}
	var ackNum int64 = 0
	if len(groupedMsg) < CriticaNum {
		for index, tempMsg = range groupedMsg {
			if msgAckSplit, err = be.ForwardMsg(index, tempMsg); err != nil {
				return nil, err
			}
			if !msgAckSplit.IsInt() {
				log.Warningf("Not the expected results")
				continue
			}
			ackNum += msgAckSplit.GetInt()
		}
		msgAck = protocol.NewIntegerMsg(ackNum)
		return
	} else {
		parts := make([]map[uint32]*protocol.Msg, GoroutineNums)
		for j := range parts {
			parts[j] = make(map[uint32]*protocol.Msg)
		}
		i := 0
		for index, msg := range groupedMsg {
			parts[i%GoroutineNums][index] = msg
			i++
		}
		f := func(msgMap map[uint32]*protocol.Msg, wg *sync.WaitGroup, ch chan<- goRet) {
			defer wg.Done()
			var innerNum int64 = 0
			var ack *protocol.Msg
			for index, msg := range msgMap {
				if ack, err = be.ForwardMsg(index, msg); err != nil {
					ch <- goRet{nil, err}
					return
				}
				innerNum += ack.GetInt()
			}
			ch <- goRet{protocol.NewIntegerMsg(innerNum), nil}
		}
		wg := &sync.WaitGroup{}
		wg.Add(GoroutineNums)
		ch := make(chan goRet, GoroutineNums)
		for i := 0; i < GoroutineNums; i++ {
			go f(parts[i], wg, ch)
		}
		wg.Wait()
		close(ch)
		for c := range ch {
			if c.err != nil {
				return nil, err
			}
			ackNum += c.s.GetInt()
		}
		msgAck = protocol.NewIntegerMsg(ackNum)
		return
	}
}

func DelProc(be backend.Backend, msg *protocol.Msg) (msgAck *protocol.Msg, err error) {
	return procDelOrExists(be, msg)
}

func ExistsProc(be backend.Backend, msg *protocol.Msg) (msgAck *protocol.Msg, err error) {
	return procDelOrExists(be, msg)
}
