package cluster

import (
	"ncache/backend/nodes"
	"sync"
)

const (
	MaxSlotsNum = 1 << 14
)

type slotInfo struct {
	num uint16
	sync.RWMutex
	slots map[uint16]*nodes.Node
}

func NewSlotInfo(slotsNum uint16) *slotInfo {
	return &slotInfo{
		num:   slotsNum,
		slots: make(map[uint16]*nodes.Node),
	}
}

func (s *slotInfo) addSlot(slot int64, node *nodes.Node) {
	s.Lock()
	defer s.Unlock()
	s.slots[uint16(slot)] = node
}

func (s *slotInfo) addSlots(begin, end int64, node *nodes.Node) {
	for ; begin <= end; begin++ {
		s.addSlot(begin, node)
	}
}

func (s *slotInfo) isValid() bool {
	s.RLock()
	defer s.RUnlock()
	if uint16(len(s.slots)) != s.num {
		return false
	}
	var i uint16
	for i = 0; i < s.num; i++ {
		if node, ok := s.slots[i]; !ok || node == nil {
			return false
		}
	}
	return true
}

func (s *slotInfo) getSlot(slot uint16) (node *nodes.Node, ok bool) {
	s.RLock()
	defer s.RUnlock()
	node, ok = s.slots[slot]
	return
}

func (s *slotInfo) getSlotLen() int {
	s.RLock()
	defer s.RUnlock()
	return len(s.slots)
}
