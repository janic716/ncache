package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/janic716/golib/log"
	"ncache/backend"
	"ncache/backend/route"
	"ncache/filter"
	"ncache/protocol"
	"ncache/utils"
	"runtime/debug"
)

type ClientMode int64
type ProcessStage int

const (
	defaultReadBufferSize  = 512
	defaultWriteBufferSize = 512
)
const (
	processReceive ProcessStage = iota
	processParse
	processRoute
	processBackend
	processResponse
)

var (
	errClientClosed   = errors.New("Server: client closed")
	errInvalidRequest = errors.New("Server: Invalid request")
	errNoDbSpecified  = errors.New("Server: No DB specified")
)

var (
	clientSeq uint64
)

func init() {
	clientSeq = 1
}

func newClientId() uint64 {
	return atomic.AddUint64(&clientSeq, uint64(1))
}

type Client struct {
	id   uint64
	addr string
	conn net.Conn
	br   *bufio.Reader
	bw   *bufio.Writer

	Server *Server

	backend  backend.Backend
	beCache  map[string]backend.Backend
	curIndex string

	stage ProcessStage

	transReqList []*protocol.Msg
	transStart   time.Time

	curCmd string
	curReq *protocol.Msg
	args   []string
	argc   int

	response *protocol.Msg

	closed          bool
	lastinteraction int64

	start time.Time
}

func NewClient(server *Server, conn net.Conn) (client *Client, err error) {
	if !server.IsStop() {
		client = &Client{
			id:      newClientId(),
			Server:  server,
			conn:    conn,
			br:      bufio.NewReaderSize(conn, defaultReadBufferSize),
			bw:      bufio.NewWriterSize(conn, defaultWriteBufferSize),
			beCache: make(map[string]backend.Backend),
			addr:    utils.RemoteAddr(conn),
			closed:  false,
		}
	} else {
		err = errors.New("server has stopped")
	}
	return
}

//todo:
func (this *Client) Close() {
	if this.IsClosed() {
		return
	}
	this.closed = true
	this.conn.Close()
	return
}

func (this *Client) IsClosed() bool {
	return this.closed
}

func (this *Client) IsIdle(idle int64) bool {
	now := utils.UnixTime()
	return now-this.lastinteraction > idle
}

func printElapse(stage string, start time.Time) {
	fmt.Println(stage, time.Now().Sub(start))
}

//todo: 阻塞读
func (this *Client) CmdReceive() (err error) {
	//fmt.Println("CmdReceive")
	this.start = time.Now()
	defer func() {
		this.lastinteraction = utils.UnixTime()
	}()
	var msg *protocol.Msg
	if msg, err = protocol.NewMsgFromReader(this.br); err != nil {
		if err != io.EOF {
			err = fmt.Errorf("read request: %s", err)
		}
		return
	}
	if !msg.IsBulkStringArray() {
		err = errInvalidRequest
		return
	}
	this.curReq = msg
	this.stage = processParse
	log.Debugf("Request:\n%s", this.curReq)
	return
}

//todo:
func (this *Client) CmdParse() (err error) {
	//fmt.Println("CmdParse")
	if this.stage != processParse {
		return
	}
	var args []string
	if args, err = this.curReq.Args(); err != nil {
		return
	}
	this.args = args
	this.curCmd = strings.ToUpper(args[0])
	if !filter.IsValidCmd(this.curCmd) {
		this.response = protocol.NewErrorMsgFmt("ERR unknown command '%s'", this.curCmd)
		this.stage = processResponse
		log.Warningf("Unsurport commnad %s", this.curCmd)
		return
	}
	if this.curCmd == "PING" {
		this.response = protocol.MsgPONG
		this.stage = processResponse
		return
	}
	this.argc = len(args)
	this.stage = processRoute
	return
}

//todo:
func (this *Client) NodeRoute() (err error) {
	//fmt.Println("NodeRoute")
	if this.stage != processRoute {
		return
	}
	if this.argc < 2 {
		this.stage = processResponse
		this.response = protocol.NewErrorMsgFmt("ERR wrong number of arguments for '%s' command", this.curCmd)
		return
	}
	defer func() {
		this.stage = processBackend
	}()

	mainKey := this.args[1]
	index := route.GetIndex(mainKey)

	if index == this.curIndex {
		if this.backend != nil {
			return
		}
		if node, ok := this.beCache[index]; ok {
			this.backend = node
			return
		}
	}

	if be, ok := this.beCache[index]; ok {
		this.backend = be
	} else {
		be, err = route.GetBackend(index)
		if err != nil {
			return err
		}
		this.backend = be
		this.beCache[index] = be
	}
	this.curIndex = index
	return
}

//todo:
func (this *Client) BackendProc() (err error) {
	//fmt.Println("BackendProc")
	if this.stage != processBackend {
		return
	}
	defer func() { this.stage = processResponse }()
	this.response, err = this.backend.Proc(this.curReq)
	return err
}

//todo:
func (this *Client) Response() (err error) {
	//fmt.Println("Response")
	log.Debugf("Response: %s", this.response)
	if this.stage != processResponse {
		return
	}
	defer func() {
		this.lastinteraction = utils.UnixTime()
		this.stage = processReceive
	}()
	if this.response == nil {
		this.response = protocol.NewErrorMsg("Err empty response")
	}
	msg := this.response
	//defer protocol.PutMsg(msg)
	if err = msg.WriteMsg(this.bw); err != nil {
		if err != io.EOF {
			cost := (time.Now().Sub(this.start).Nanoseconds()) / 1000
			err = fmt.Errorf("request: %s time: %d | response: %s", this.curReq, cost, err)
		}
		return
	}
	log.Debugf("Response:\n%s", this.response)
	return err
}

func clientHandler(client *Client) {
	defer func() {
		if r := recover(); r != nil {
			log.Error("clientHanler panic", r, "stack", string(debug.Stack()))
		}
		client.Close()
	}()
	server := client.Server
	var (
		err    error
		errTag string
	)
	if err = server.postFrontConnectHandler(client); err != nil {
		errTag = "after connect"
		goto errHandle
	}
	for {
		if err = client.CmdReceive(); err != nil {
			errTag = "command receive"
			goto errHandle
		}
		if err = server.postCommandReceiveHandler(client); err != nil {
			errTag = "after command receive"
			goto errHandle
		}
		if err = client.CmdParse(); err != nil {
			errTag = "command paser"
			goto errHandle
		}
		if err = server.postCommandParseHandler(client); err != nil {
			errTag = "after command parse"
			goto errHandle
		}
		if err = client.NodeRoute(); err != nil {
			errTag = "node route"
			goto errHandle
		}
		if err = server.postNodeRouteHandler(client); err != nil {
			errTag = "after node route"
			goto errHandle
		}
		if err = client.BackendProc(); err != nil {
			errTag = "backend proc"
			goto errHandle
		}
		if err = server.postBackendProcHandler(client); err != nil {
			errTag = "after backend proc"
			goto errHandle
		}
		if err = client.Response(); err != nil {
			errTag = "response"
			goto errHandle
		}
		if err = server.postFrontResponseHandler(client); err != nil {
			errTag = "after response"
			goto errHandle
		}
	}
errHandle:
	if err != nil {
		if err != io.EOF {
			log.Errorf("[client][%s] %s err: %s", client.addr, errTag, err)
		}
		client.Close()
	}
}
