package config

import (
	"errors"
	"fmt"
	"github.com/janic716/golib/log"
	"strings"
)

const (
	TypeSlice   = "slice"
	TypeCluster = "cluster"
)

var (
	defaultDBConfPath = "config/db.json"
)

var (
	defaultName             = "default"
	defaultAddress          = "127.0.0.1"
	defaultServerPort       = 10000
	defaultMonitorPort      = 10001
	defaultMaxClient        = 5000
	defaultMaxClientIdle    = 300
	defaultTimeTaskInterval = 60
)

var (
	defaultModule       = "Ncache"
	defaultLevel        = "ERROR,INFO"
	defaultLogDir       = "./"
	defaultSuffixFormat = ""
	defaultMaxLogCount  = 5000
)

//todo:

type NodeConf struct {
	Mode   byte `json:"mode"`
	Name   string
	Weight int
	Master *DbConf   `json:"master"`
	Slaves []*DbConf `json:"slaves"`
}

func (node *NodeConf) String() string {
	s := ""
	s += fmt.Sprintf("Mode: %d,", node.Mode)
	s += fmt.Sprintf("Master: %s", node.Master.Addr)
	if len(node.Slaves) <= 0 {
		return s
	}
	s += fmt.Sprint(",Slave:")
	for i, slave := range node.Slaves {
		s += fmt.Sprintf("%d->%s", i, slave.Addr)
	}
	return s
}

type DbConf struct {
	Role         string `json:"role"`
	Addr         string `json:"addr"`
	User         string `json:"user"`
	Pass         string `json:"pass"`
	InitConnNum  int    `json:"init_conn_num"`
	MaxConnNum   int    `json:"max_conn_num"`
	ConnTimeout  int    `json:"conn_timeout"`
	ReadTimeout  int    `json:"read_timeout"`
	WriteTimeout int    `json:"write_timeout"`
}

type ServerConf struct {
	Name             string `json:"name"`
	Address          string `json:"address"`
	ServerPort       int    `json:"server_port"`
	MonitorPort      int    `json:"monitor_port"`
	MaxClient        int    `json:"max_client"`
	MaxClientIdle    int    `json:"max_client_idle"`
	TimeTaskInterval int    `json:"time_task_interval"`
	PprofEnable      bool   `json:"pprof_enable"`
	PprofAddr        string `json:"pprof_addr"`
}

type Conf interface {
	GetType() string
}

type ClusterConf struct {
	Name            string `josn:"name"`
	Mode            byte   `json:"mode"`
	Type            string `json:"type"`
	SlotNum         uint16 `json:"slot_num"`
	MaxRetry        int    `json:"retry_times"`
	RefreshInterval int    `json:"refresh_interval"`
	// 以下四项长度需相等，如某些主节点没有从节点，则对应位置需填上空字符串
	// 节点地址以IP:PORT的形式输入，当节点有多个从节点时，应以','分割
	Masters      []string `json:"masters"`
	Slaves       []string `json:"slaves`
	InitConnNum  int      `json:"init_conn_num"`
	MaxConnNum   int      `json:"max_conn_num"`
	ConnTimeout  int      `json:"conn_timeout"`
	ReadTimeout  int      `json:"read_timeout"`
	WriteTimeout int      `json:"write_timeout"`
}

func (c ClusterConf) GetType() string {
	return c.Type
}

type SliceConf struct {
	Name             string `json:"name"`
	Mode             byte   `json:"mode"`
	Type             string `json:"type"`
	Hash             string `json:"hash"`
	Preconnect       bool   `json:"pre_connect"`
	Distribution     string `json:"distribution"`
	auto_eject_hosts bool   `json:"auto_eject"`
	// 以下四项长度需相等，如某些主节点没有从节点，则对应位置需填上空字符串
	// 节点地址以IP:PORT的形式输入，当节点有多个从节点时，应以','分割
	Masters      []string `json:"masters"`
	Slaves       []string `json:"slaves"`
	Weights      []int    `json:"weights"`
	NodeNames    []string `json:"node_name"`
	InitConnNum  int      `json:"init_conn_num"`
	MaxConnNum   int      `json:"max_conn_num"`
	ConnTimeout  int      `json:"conn_timeout"`
	ReadTimeout  int      `json:"read_timeout"`
	WriteTimeout int      `json:"write_timeout"`
}

func (s SliceConf) GetType() string {
	return s.Type
}

func (s *SliceConf) GetWeights() []int {
	return s.Weights
}

func (s *SliceConf) GetNodeNames() []string {
	return s.NodeNames
}

func GetNodesConf(value interface{}) (nodes []NodeConf, err error) {
	switch conf := value.(type) {
	case SliceConf:
		if len(conf.Masters) <= 0 {
			return nil, errors.New("No nodes specified")
		}
		if len(conf.Slaves) > 0 && len(conf.Masters) != len(conf.Slaves) {
			return nil, errors.New("The length of the master and slave nodes does not match")
		}

		if len(conf.Masters) != len(conf.Weights) || len(conf.Masters) != len(conf.NodeNames) {
			return nil, errors.New("Invlid Slice conf")
		}
		initConn := conf.InitConnNum
		maxConn := conf.MaxConnNum
		cout := conf.ConnTimeout
		rout := conf.ReadTimeout
		wout := conf.WriteTimeout
		for i := 0; i < len(conf.Masters); i++ {
			node := NodeConf{
				Mode:   conf.Mode,
				Name:   conf.NodeNames[i],
				Weight: conf.Weights[i],
			}
			master := dbConfHelpFunc(initConn, maxConn, cout, rout, wout)
			master.Role = "master"
			master.Addr = conf.Masters[i]
			node.Master = &master
			if len(conf.Slaves) == 0 || len(conf.Slaves[i]) == 0 {
				nodes = append(nodes, node)
				continue
			}
			slaveAddrs := strings.Split(conf.Slaves[i], ",")
			for _, slaveAddr := range slaveAddrs {
				slave := dbConfHelpFunc(initConn, maxConn, cout, rout, wout)
				slave.Role = "slave"
				slave.Addr = slaveAddr
				node.Slaves = append(node.Slaves, &slave)
			}
			nodes = append(nodes, node)
		}
	case ClusterConf:
		if len(conf.Masters) <= 0 {
			return nil, errors.New("No nodes specified")
		}
		if len(conf.Slaves) > 0 && len(conf.Masters) != len(conf.Slaves) {
			return nil, errors.New("The length of the master and slave nodes does not match")
		}
		mode := conf.Mode
		initConn := conf.InitConnNum
		maxConn := conf.MaxConnNum
		cout := conf.ConnTimeout
		rout := conf.ReadTimeout
		wout := conf.WriteTimeout
		for i := 0; i < len(conf.Masters); i++ {
			node := NodeConf{Mode: mode}
			master := dbConfHelpFunc(initConn, maxConn, cout, rout, wout)
			master.Role = "master"
			master.Addr = conf.Masters[i]
			node.Master = &master
			if len(conf.Slaves) == 0 || len(conf.Slaves[i]) == 0 {
				nodes = append(nodes, node)
				continue
			}
			slaveAddrs := strings.Split(conf.Slaves[i], ",")
			for _, slaveAddr := range slaveAddrs {
				slave := dbConfHelpFunc(initConn, maxConn, cout, rout, wout)
				slave.Role = "slave"
				slave.Addr = slaveAddr
				node.Slaves = append(node.Slaves, &slave)
			}
			nodes = append(nodes, node)
		}

	}
	return nodes, nil
}

func dbConfHelpFunc(initConn, maxConn, cout, rout, wout int) DbConf {
	return DbConf{"", "", "", "", initConn, maxConn, cout, rout, wout}
}

// todo check and adjust return false when serious problem, else check and adjust then return true
func (conf *NodeConf) checkIsValid() bool {
	if conf == nil {
		return false
	}
	// todo fix invalid conn num or timeout
	return true
}

func mergeServerConf(conf1, conf2 *ServerConf) (err error) {
	if conf1 == nil || conf2 == nil {
		err = errors.New("Server Conf is Nil")
		return
	}
	if conf2.Name != "" {
		conf1.Name = conf2.Name
	}
	if conf2.Address != "" {
		conf1.Address = conf2.Address
	}
	if conf2.ServerPort != 0 {
		conf1.ServerPort = conf2.ServerPort
	}
	if conf2.MonitorPort != 0 {
		conf1.MonitorPort = conf2.MonitorPort
	}
	if conf2.MaxClient != 0 {
		conf1.MaxClient = conf2.MaxClient
	}
	if conf2.MaxClientIdle != 0 {
		conf1.MaxClientIdle = conf2.MaxClientIdle
	}
	if conf2.TimeTaskInterval != 0 {
		conf1.TimeTaskInterval = conf2.TimeTaskInterval
	}
	return nil
}

func mergeLogConf(conf1, conf2 *log.LogConf) (err error) {
	if conf1 == nil || conf2 == nil {
		err = errors.New("Server Conf is Nil")
		return
	}
	if conf2.Module != "" {
		conf1.Module = conf2.Module
	}
	if conf2.Level != "" {
		conf1.Level = conf2.Level
	}
	if conf2.LogDir != "" {
		conf1.LogDir = conf2.LogDir
	}
	if conf2.SuffixFormat != "" {
		conf1.SuffixFormat = conf2.SuffixFormat
	}
	if conf2.MaxLogCount != 0 {
		conf1.MaxLogCount = conf2.MaxLogCount
	}
	return nil
}
