package cluster

import (
	"bytes"
	"ncache/utils"
)

func hashSlot(key []byte, mask uint16) uint16 {
	buf := []byte(key)
	if leftIndex := bytes.IndexByte(buf, byte('{')); leftIndex != -1 {
		if rightIndex := bytes.IndexByte(buf[leftIndex+1:], byte('}')); rightIndex > 0 {
			buf = buf[leftIndex+1 : leftIndex+rightIndex+1]
		}
	}
	return utils.Crc16(buf) & mask
}
