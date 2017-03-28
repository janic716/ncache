package nodes

import "sync/atomic"

//todo: 负载均衡算法
type balanceFunc func(node *Node) int

func normalBalance(node *Node) int {
	index := int(atomic.AddInt32(&node.lastSalveIndex, 1)) % node.SlaveLen()
	return index
}
