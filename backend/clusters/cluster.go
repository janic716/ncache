package cluster

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/janic716/golib/log"
	"ncache/backend/command"
	"ncache/backend/nodes"
	"ncache/config"
	"ncache/protocol"
)

//todo: 集群支持, 优先级低

const (
	defaultRedirectTime = 3
)

var (
	defaultRefreshInterval = time.Second * 60
)

type hashFunc func(key []byte, mask uint16) uint16

type Cluster struct {
	sync.RWMutex
	conf            config.ClusterConf
	name            string
	hash            hashFunc
	nodes           []*nodes.Node
	slotNum         uint16
	mask            uint16
	redirectTime    int
	slots           *slotInfo
	atnRw           sync.RWMutex
	addrToNode      map[string]*nodes.Node
	refreshInterval time.Duration
	isClosed        bool
}

func NewClusterWithConf(conf config.ClusterConf) (c *Cluster, err error) {
	nodesConf, err := config.GetNodesConf(conf)
	if err != nil {
		return nil, err
	}
	nodeList := make([]*nodes.Node, len(nodesConf))
	for i, nodeConf := range nodesConf {
		temp := nodeConf
		node, err := nodes.NewNode(&temp)
		if err != nil {
			return nil, err
		}
		nodeList[i] = node
	}
	c, err = newCluster(conf.Name, nodeList, conf.SlotNum)
	if err != nil {
		return nil, err
	}
	c.conf = conf
	return c, nil
}

func newCluster(name string, nodeList []*nodes.Node, slotsNum uint16) (c *Cluster, err error) {
	if len(nodeList) <= 0 {
		err = errors.New("Cluster 配置节点为空")
		return
	}

	c = &Cluster{
		name:            name,
		nodes:           nodeList,
		hash:            hashSlot,
		slotNum:         slotsNum,
		mask:            slotsNum - 1,
		redirectTime:    defaultRedirectTime,
		slots:           NewSlotInfo(slotsNum),
		addrToNode:      make(map[string]*nodes.Node),
		refreshInterval: defaultRefreshInterval,
	}

	fmt.Println(nodeList)
	// 建立master地址与node的对应关系
	for _, node := range nodeList {
		//fmt.Println("node", node)
		addr := node.GetMasterAddress()
		//fmt.Println("make mpa",  addr)
		c.addrToNode[addr] = node
	}

	// 初始化slots 信息
	if err = c.refresh(); err != nil {
		return nil, err
	}
	// 定时刷新
	go func() {
		for range time.Tick(defaultRefreshInterval) {
			if err := c.refresh(); err != nil {
				log.Infof("刷新cluster slots出错。Info: %s", err.Error())
			}
		}
	}()
	return
}

func (c *Cluster) refresh() (err error) {
	index := rand.Intn(len(c.nodes))
	node := c.nodes[index]
	//todo 验证集群是否处在正常工作状态

	// 发送CLUSTER SLOTS命令获取集群槽位分配信息
	var slotsMsgAck *protocol.Msg
	if slotsMsgAck, err = node.RelayMsg(protocol.MsgClusterSllots); err != nil {
		return
	}
	if !slotsMsgAck.IsArray() || slotsMsgAck.GetArrayLen() <= 0 {
		err = errors.New("slots数据无效")
	}
	slotAssignValues := slotsMsgAck.GetArray()
	if err != nil {
		return
	}

	tempSlotInfo := NewSlotInfo(c.slotNum)

	for _, slotAssignValue := range slotAssignValues {
		var (
			low, high, port int64
			ip              string
			node            *nodes.Node
			ok              bool
		)
		if !slotAssignValue.IsArray() || slotAssignValue.GetArrayLen() < 3 {
			return errors.New("slots节点数据无效")
		}
		slotAssignInfos := slotAssignValue.GetArray()
		if !slotAssignInfos[0].IsInt() || !slotAssignInfos[1].IsInt() {
			return errors.New("slots节点起始或者结束数据无效")
		}
		if low = slotAssignInfos[0].GetInt(); err != nil {
			return errors.New("slots节点起始数据无效")
		}
		if high = slotAssignInfos[1].GetInt(); err != nil {
			return errors.New("slots节点结束数据无效")
		}

		// todo 理解逻辑时使用，后期删除
		/*
			1) 1) (integer) 10001
			   2) (integer) 16383
			   3) 1) "10.10.200.10"
				  2) (integer) 7002
				  3) "0a98df272a095032a411f2921c0e872b9c08046f"
			2) 1) (integer) 0
			   2) (integer) 5000
			   3) 1) "10.10.200.10"
				  2) (integer) 7000
				  3) "8448614173627ed4cb940f8d10cf7812f949d1a9"
			   4) 1) "10.10.200.10"
				  2) (integer) 7010
				  3) "5dff6fd4b41fbd2d8a65a82d04ccb737e4c2405b"
			3) 1) (integer) 5001
			   2) (integer) 10000
			   3) 1) "10.10.200.10"
				  2) (integer) 7001
				  3) "580837f16ed608a97ca3d0113eae5c5e911e718a"
			   4) 1) "10.10.200.10"
				  2) (integer) 6991
				  3) "7afe96d405680cec24f3ac03ee2948a7e7bada2d"
		*/
		// 当集群模式中挂了主从时，一个槽位会被主从同时使用，但是redis保证返回信息中机器列表的第一个值为master
		// 项目中后端连接逻辑已一个主从系统为单位，因此只需要解析master的槽位信息即可
		nodeInfo := slotAssignInfos[2] // master节点槽位信息
		if !nodeInfo.IsArray() || nodeInfo.GetArrayLen() < 2 {
			return errors.New("slots节点数据无效")
		}
		ipPort := nodeInfo.GetArray()
		if err != nil {
			return err
		}
		if !ipPort[0].IsBulk() {
			return errors.New("slot节点IP无效")
		}
		if ip, err = ipPort[0].GetStr(); err != nil {
			return err
		}
		if !ipPort[1].IsInt() {
			return errors.New("slot节点PORT无效")
		}
		if port = ipPort[1].GetInt(); err != nil {
			return err
		}
		addr := ip + ":" + strconv.FormatInt(port, 10)
		if node, ok = c.addrToNode[addr]; !ok {
			fmt.Println("addr", addr)
			return errors.New("集群未包含该节点")
		}

		tempSlotInfo.addSlots(low, high, node)
	}
	if !tempSlotInfo.isValid() {
		return errors.New("refresh slots信息错误")
	}
	c.RLock()
	c.slots = tempSlotInfo
	c.RUnlock()
	return
}

func (c *Cluster) Proc(msg *protocol.Msg) (ackMsg *protocol.Msg, err error) {
	methodByte, _ := msg.GetArray()[0].GetValueBytes()
	method := strings.ToUpper(string(methodByte))
	switch method {
	case "MSET":
		ackMsg, err = command.MsetProc(c, msg)
	case "MGET":
		ackMsg, err = command.MgetProc(c, msg)
	case "DEL":
		ackMsg, err = command.DelProc(c, msg)
	case "EXISTS":
		ackMsg, err = command.ExistsProc(c, msg)
	default:
		ackMsg, err = command.KeyProc(c, msg)
	}
	return ackMsg, err
}

func (c *Cluster) GetNodeByMasterAddr(addr string) (node *nodes.Node, err error) {
	c.atnRw.RLock()
	defer c.atnRw.RUnlock()
	node, ok := c.addrToNode[addr]
	if !ok {
		return nil, fmt.Errorf("集群为包含地址为%s的主节点", addr)
	}
	return node, nil
}

func (c *Cluster) GetNodeByKey(key []byte) (node *nodes.Node, err error) {
	slot := c.hash(key, c.mask)
	node, ok := c.slots.slots[slot]
	if !ok {
		return nil, fmt.Errorf("槽位 %d 未分配", slot)
	}
	return node, nil
}

func (c *Cluster) GetNodeIndexByKey(key []byte) (index uint32) {
	return uint32(c.hash(key, c.mask))
}

func (c *Cluster) GetNodes() []*nodes.Node {
	return c.nodes
}

func (c *Cluster) GetConf() interface{} {
	return c.conf
}
