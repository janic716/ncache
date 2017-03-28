package server

import (
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/janic716/golib/log"
	"ncache/config"
)

type Server struct {
	conf              *config.ServerConf
	addr              string
	listener          net.Listener
	mutex             sync.RWMutex
	clients           map[uint64]*Client
	aspectList        []NcacheAspect
	stop              bool
	timerTaskInterval int
	maxClientIdleTime int64
}

//todo:
func NewServer() (server *Server, err error) {
	server = &Server{
		clients: make(map[uint64]*Client),
	}
	conf, err := config.GetServerConf()
	if err != nil {
		log.Error("server configurateion initialize failed")
	}
	addr := conf.Address
	port := conf.ServerPort
	address := strings.Join([]string{addr, strconv.FormatInt(int64(port), 10)}, ":")
	if server.listener, err = net.Listen("tcp", address); err != nil {
		log.Infof("new server listen failed, addr: %s, err: %s", address, err)
		return nil, err
	}
	server.conf = conf
	server.addr = address
	server.timerTaskInterval = conf.TimeTaskInterval
	server.maxClientIdleTime = int64(conf.MaxClientIdle)
	log.Infof("[server]new server starting, addr: %s", address)
	return
}

//todo:
func (this *Server) initAspect() {
	if this.aspectList == nil {
		this.aspectList = make([]NcacheAspect, 0, 2)
	}
}

//todo:
func (this *Server) initStatus() {

}

func (this *Server) AddAspect(aspect NcacheAspect) {
	if this.stop {
		this.aspectList = append(this.aspectList, aspect)
	}
}

func (this *Server) Run() {
	this.initStatus()
	this.stop = false
	go this.timerTask()
	for !this.stop {
		if conn, err := this.listener.Accept(); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(10 * time.Millisecond)
				continue
			}
			log.Errorf("[server] accept err: %s", err)
		} else {
			if client, err := NewClient(this, conn); err == nil {
				this.addClient(client)
				go clientHandler(client)
			} else {
				log.Errorf("[server] create client err: %s", err)
			}
		}
	}
	this.listener = nil
}

func (this *Server) addClient(client *Client) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	this.clients[client.id] = client
}

func (this *Server) removeClient(client *Client) {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	delete(this.clients, client.id)
}

func (this *Server) removeClientList(clientList []*Client) {
	if len(clientList) > 0 {
		this.mutex.Lock()
		defer this.mutex.Unlock()
		for _, client := range clientList {
			delete(this.clients, client.id)
		}
	}
}

//todo: 定时任务
func (this *Server) timerTask() {
	for !this.stop {
		for {
			if this.timerTaskInterval == 0 {
				this.timerTaskInterval = 10
			}
			select {
			case <-time.Tick(time.Duration(this.timerTaskInterval) * time.Second):
				this.clearClosedClients()
				this.clearIdleClients()
				this.clearTimeoutClients()
			}
		}
	}
}

//todo: 清理已关闭的client
func (this *Server) clearClosedClients() {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	for k, c := range this.clients {
		if c.IsClosed() {
			delete(this.clients, k)
		}
	}
}

//todo: 清理空闲的client
func (this *Server) clearIdleClients() {
	this.mutex.Lock()
	defer this.mutex.Unlock()
	for k, c := range this.clients {
		if c.IsIdle(this.maxClientIdleTime) {
			delete(this.clients, k)
		}
	}
}

//todo: 清理超时的client
func (this *Server) clearTimeoutClients() {

}

func (this *Server) Close() {
	this.stop = true

}

func (this *Server) IsStop() bool {
	return this.stop
}

func (this *Server) postFrontConnectHandler(client *Client) (err error) {
	if client == nil || client.IsClosed() {
		return errClientClosed
	}
	for _, aspect := range this.aspectList {
		if err = aspect.PostFrontConnect(client); err != nil {
			return
		}
	}
	return
}

func (this *Server) postCommandReceiveHandler(client *Client) (err error) {
	if client == nil || client.IsClosed() {
		return errClientClosed
	}
	for _, aspect := range this.aspectList {
		if err = aspect.PostCommandReceive(client); err != nil {
			return
		}
	}
	return
}
func (this *Server) postCommandParseHandler(client *Client) (err error) {
	if client == nil || client.IsClosed() {
		return errClientClosed
	}
	for _, aspect := range this.aspectList {
		if err = aspect.PostCommandParse(client); err != nil {
			return
		}
	}
	return
}

func (this *Server) postNodeRouteHandler(client *Client) (err error) {
	if client == nil || client.IsClosed() {
		return errClientClosed
	}
	for _, aspect := range this.aspectList {
		if err = aspect.PostNodeRoute(client); err != nil {
			return
		}
	}
	return
}

func (this *Server) postBackendProcHandler(client *Client) (err error) {
	if client == nil || client.IsClosed() {
		return errClientClosed
	}
	for _, aspect := range this.aspectList {
		if err = aspect.PostBackendProc(client); err != nil {
			return
		}
	}
	return
}
func (this *Server) postFrontResponseHandler(client *Client) (err error) {
	for _, aspect := range this.aspectList {
		if err = aspect.PostFrontResponse(client); err != nil {
			return
		}
	}
	return
}
