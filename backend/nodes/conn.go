package nodes

import (
	"bufio"
	"errors"
	"ncache/config"
	"ncache/protocol"
	"ncache/utils"
	"io"
	"net"
	"sync"
	"time"
)

var (
	errConnectClosed = errors.New("connect closed")
	errDbClosed      = errors.New("db closed")
)

type PingFunc func(reader io.Reader, writer io.Writer) error

const (
	defaultConnTimeout  = 3000
	defaultReadTimeout  = 3000
	defaultWriteTimeout = 3000

	connStatusConnected    = 0 //connection that connected
	connStatusClosed       = 1 //connection that closed
	connStatusSick         = 2 //connection that possible closed
	defaultReadBufferSize  = 512
	defaultWriteBufferSize = 512

	defaultFlag  int = 1 << 0
	askingFlag   int = 1 << 1
	readonlyFlag int = 1 << 2
)

type Conn struct {
	addr         string
	conn         net.Conn
	activeTime   int64
	connTimeout  int
	readTimeout  int
	writeTimeout int
	status       int
	pingFunc     PingFunc
	br           *bufio.Reader
	bw           *bufio.Writer
	flag         int
	sync.RWMutex
}

func NewConn(addr string, pingFunc PingFunc, dbConf *config.DbConf) (conn *Conn, err error) {
	conn = &Conn{
		addr:         addr,
		connTimeout:  dbConf.ConnTimeout,
		readTimeout:  dbConf.ReadTimeout,
		writeTimeout: dbConf.WriteTimeout,
		pingFunc:     pingFunc,
		flag:         defaultFlag,
		status:       1,
	}
	if conn.connTimeout <= 0 {
		conn.connTimeout = defaultConnTimeout
	}
	if conn.readTimeout <= 0 {
		conn.readTimeout = defaultReadTimeout
	}
	if conn.writeTimeout <= 0 {
		conn.writeTimeout = defaultWriteTimeout
	}
	if conn.pingFunc == nil {
		conn.pingFunc = func(reader io.Reader, writer io.Writer) error {
			return nil
		}
	}
	err = conn.Connect()
	return
}

func (this *Conn) Connect() error {
	if this.conn != nil {
		this.conn.Close()
	}
	//netConn, err := net.DialTimeout("tcp", this.addr, time.Duration(this.connTimeout)*time.Microsecond)
	netConn, err := net.DialTimeout("tcp", this.addr, time.Duration(this.connTimeout)*time.Millisecond)
	if err != nil {
		return err
	}
	tcpConn := netConn.(*net.TCPConn)
	tcpConn.SetNoDelay(false)
	tcpConn.SetKeepAlive(true)
	tcpConn.SetReadBuffer(1024)
	tcpConn.SetWriteBuffer(1024)
	this.conn = tcpConn
	this.status = connStatusConnected
	this.br = bufio.NewReaderSize(this, defaultReadBufferSize)
	this.bw = bufio.NewWriterSize(this, defaultWriteBufferSize)
	this.updateActiveTime()
	//fmt.Println("connected")
	return nil
}

func (this *Conn) HandleMsg(req *protocol.Msg) (rsp *protocol.Msg, err error) {
	if this.IsClosed() {
		return nil, errConnectClosed
	}
	if err = this.conn.SetWriteDeadline(time.Now().Add(time.Duration(this.writeTimeout) * time.Millisecond)); err != nil {
		return nil, err
	}
	if err = req.WriteMsg(this.bw); err != nil {
		return nil, err
	}
	if err = this.conn.SetReadDeadline(time.Now().Add(time.Duration(this.readTimeout) * time.Millisecond)); err != nil {
		return nil, err
	}
	if rsp, err = protocol.NewMsgFromReader(this.br); err != nil {
		return nil, err
	}
	return
}

func (this *Conn) Read(data []byte) (n int, err error) {
	if this.IsClosed() {
		return 0, errConnectClosed
	}
	if this.readTimeout > 0 {
		if err = this.conn.SetReadDeadline(time.Now().Add(time.Duration(this.readTimeout) * time.Millisecond)); err != nil {
			this.Close()
			return n, err
		}
	}
	if n, err = this.conn.Read(data); err == nil {
		this.updateActiveTime()
	} else {
		if err == io.EOF {
			this.Close()
		} else {
			this.status = connStatusSick
		}
	}
	return
}

func (this *Conn) Write(cmd []byte) (n int, err error) {
	if this.IsClosed() {
		return 0, errConnectClosed
	}
	if this.writeTimeout > 0 {
		if err = this.conn.SetWriteDeadline(time.Now().Add(time.Duration(this.readTimeout) * time.Millisecond)); err != nil {
			this.Close()
			return n, err
		}
	}
	if n, err = this.conn.Write(cmd); err == nil {
		this.updateActiveTime()
	} else {
		this.status = connStatusSick
		this.Close()
	}
	return
}

func (this *Conn) updateActiveTime() {
	this.activeTime = utils.UnixTime()
}

func (this *Conn) Ping() (err error) {
	err = this.pingFunc(this.br, this.bw)
	if err != nil {
		this.Close()
	}
	return err
}

func (this *Conn) Close() error {
	this.Lock()
	defer this.Unlock()
	if this.status != connStatusClosed {
		err := this.conn.Close()
		this.status = connStatusClosed
		return err
	}
	//fmt.Println("con closed:", this.addr)
	return nil
}

func (this *Conn) IsClosed() bool {
	return this.status == connStatusClosed
}

func (this *Conn) IsSick() bool {
	return this.status == connStatusSick
}

func (this *Conn) SendReadOnly() (err error) {
	if this.flag&readonlyFlag != 0 {
		return
	}
	if _, err = this.HandleMsg(protocol.MsgReadOnly); err != nil {
		return err
	}
	this.flag |= readonlyFlag
	return
}

func (this *Conn) SendAsking() (err error) {
	if this.flag&askingFlag != 0 {
		return
	}
	if _, err = this.HandleMsg(protocol.MsgAsking); err != nil {
		return err
	}
	this.flag |= askingFlag
	return
}
