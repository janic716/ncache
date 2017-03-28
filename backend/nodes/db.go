package nodes

import (
	"errors"
	"fmt"
	"ncache/config"
	"ncache/protocol"
	"ncache/utils"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DbStatusUP     = iota //正常工作
	DbStatusDown          //停止工作
	DbStatusReload        //重新载入

	defaultInitConnNum = 10
	defaultMaxConnNum  = 100

	descErrGetConnect = "get connect failed"

	pingInterval = 10 //s

	maxIdleTime = 300 //s

	taskCloseIdleConnNum  = 3
	idleConnCheckInterval = 60 * time.Second //s
	dbCheckInterval       = 5 * time.Second  //s

	roleMaster = "master"
	roleSlave  = "slave"
)

var (
	errInitDb     = errors.New("init db failed")
	errDbDown     = errors.New("db down")
	errNoIdleConn = errors.New("no idle conn")
)

//redis 实例, 维护长连接
type Db struct {
	conf          *config.DbConf
	role          string
	addr          string
	user          string
	pass          string
	initConnChan  chan *Conn
	extraConnChan chan *Conn
	checkConn     *Conn
	//sickConnChan  chan *Conn
	initConnNum     int
	maxConnNum      int
	curWorkConnNum  int32 //current work conn num
	status          int
	chanRWMutex     sync.RWMutex
	connCreateMutex sync.RWMutex
	busy            int
}

func NewDb(conf *config.DbConf) (db *Db, err error) {
	db = &Db{conf: conf, addr: conf.Addr, user: conf.User, pass: conf.Pass, initConnNum: conf.InitConnNum, maxConnNum: conf.MaxConnNum}
	if conf.Role == "master" {
		db.role = roleMaster
	} else {
		db.role = roleSlave
	}
	err = db.initDb()
	tryTimes := 5
	msWaitTime := 1000
	go utils.TimerTask(func() error {
		return db.healthCheck(tryTimes, msWaitTime)
	}, dbCheckInterval)
	go utils.TimerTask(db.idleConnCheck, idleConnCheckInterval)
	return
}


func (this *Db) initDb() (err error) {
	if _, err = this.createCheckConn(); err != nil {
		return errInitDb
	}
	if this.initConnNum <= 0 {
		this.initConnNum = defaultInitConnNum
	}
	if this.maxConnNum == 0 {
		this.maxConnNum = utils.MaxInt(this.initConnNum, defaultMaxConnNum)
	}
	this.initConnChan = make(chan *Conn, this.maxConnNum)
	this.extraConnChan = make(chan *Conn, this.maxConnNum)
	initConnFn := func() error {
		_, err := this.createWorkConn(true)
		return err
	}
	num := utils.MaxInt(1, this.initConnNum/4)
	utils.Execute(initConnFn, num) //先创建部分连接, 再异步创建其他连接, 加快启动速度
	go utils.Execute(initConnFn, this.initConnNum-num)
	if this.curWorkConnNum == 0 {
		return errInitDb
	}
	this.status = DbStatusUP
	fmt.Printf("[init db] addr:%s, db is up\n", this.addr)
	return nil
}

//todo: 简单实现
func (this *Db) healthCheck(retryTime, msWaitTime int) (err error) {
	checkConn := this.getCheckConn()
	if this.status == DbStatusUP {
		if checkConn == nil {
			checkConn, err = this.createCheckConn()
		}
		if err == nil || checkConn.Ping() == nil {
			fmt.Printf("[check db] addr:%s, db is up, current conn num: %d, initchan: %d, extrachan: %d\n", this.addr, this.curWorkConnNum, len(this.initConnChan), len(this.extraConnChan))
			return
		}
		if status := utils.RetryExecuteWithWait(func() error {
			var err error
			if checkConn, err = this.createCheckConn(); err == nil {
				return checkConn.Ping()
			} else {
				return err
			}
		}, retryTime, msWaitTime); !status {
			this.CloseDb()
		}
		fmt.Printf("[check db] addr:%s, db is up\n", this.addr)
	} else if this.status == DbStatusDown {
		this.initDb()
		fmt.Printf("[check db] addr:%s, db is down\n", this.addr)
	}
	return nil
}

func (this *Db) createCheckConn() (*Conn, error) {
	this.connCreateMutex.Lock()
	this.connCreateMutex.Unlock()
	var err error
	this.checkConn, err = this.newDbConn()
	return this.checkConn, err
}

func (this *Db) closeCheckConn() {
	checkConn := this.getCheckConn()
	if checkConn != nil {
		checkConn.Close()
	}
}

func (this *Db) newDbConn() (*Conn, error) {
	return NewConn(this.addr, protocol.Ping, this.conf)
}

func (this *Db) idleConnCheck() error {
	if this.status == DbStatusDown {
		return errDbDown
	}
	if this.status == DbStatusReload {
		return nil
	}
	currentWorkConnNum := int(this.curWorkConnNum)
	closeCount := 0
	if currentWorkConnNum > this.initConnNum {
		extraConnChan := this.getExtraConnChan()

		if extraConnChan != nil {
			for i := 0; i < taskCloseIdleConnNum; i++ {
				select {
				case conn := <-extraConnChan:
					if conn != nil && utils.UnixTime()-conn.activeTime > maxIdleTime {
						closeCount++
						this.closeWorkConn(conn)
					} else {
						this.PutConn(conn)
					}
				default:
					return nil
				}
			}
		}
	} else if currentWorkConnNum < this.initConnNum {
		utils.Execute(func() error {
			_, err := this.createWorkConn(true)
			return err
		}, this.initConnNum-int(this.curWorkConnNum))
	}
	this.busy = utils.MaxInt(currentWorkConnNum-this.initConnNum, 0) * 100 / this.initConnNum
	return nil
}

func (this *Db) createWorkConn(needPutChan bool) (*Conn, error) {
	if this.status == DbStatusDown {
		return nil, errDbDown
	}
	this.connCreateMutex.Lock()
	defer this.connCreateMutex.Unlock()
	if int(this.curWorkConnNum) >= this.maxConnNum {
		return nil, fmt.Errorf("conn over limit, current conn num: %d", this.curWorkConnNum)
	}
	if conn, err := this.newDbConn(); err != nil {
		return nil, err
	} else {
		atomic.AddInt32(&this.curWorkConnNum, 1)
		if needPutChan {
			this.putConn(conn)
		}
		fmt.Printf("create conn, addr:%s, local addr:%s\n", this.addr, conn.conn.LocalAddr())
		return conn, nil
	}
}

func (this *Db) GetConnect() (*Conn, error) {
	var (
		conn *Conn
		err  error
	)
	if this.status == DbStatusDown {
		return nil, errDbDown
	}
	var (
		firstChan  chan *Conn
		secondChan chan *Conn
	)
	if utils.RandomTrue(this.busy) {
		firstChan = this.getExtraConnChan()
		secondChan = this.getInitConnChan()
	} else {
		firstChan = this.getInitConnChan()
		secondChan = this.getExtraConnChan()
	}
	//conn = this.popConnFromConnChan(firstChan, 3, 0)
	//if conn == nil {
	//	conn = this.popConnFromConnChan(secondChan, 3, 100)
	//}
	conn, err = this.popConn(firstChan, secondChan, 10, 0)
	if err != nil {
		_, err = this.createWorkConn(true)
		conn, err = this.popConn(firstChan, secondChan, 3, 10)
	}

	//conn = this.popConn(firstChan, secondChan, 3, 10)
	//if err != nil && int(this.curWorkConnNum) < this.maxConnNum {
	//	conn, err = this.createWorkConn(false)
	//} else {
	//	conn, err = this.popConn(firstChan, secondChan, 1, 10)
	//}
	//conn, err = this.popConn(firstChan, secondChan, 1, 10)
	//if err == nil {
	//	conn, err = this.createWorkConn(false)
	//}
	return conn, err
}

func (this *Db) popConn(chan1, chan2 <-chan *Conn, tryTimes int, msWaitTime int) (conn *Conn, err error) {
	if chan1 == nil || chan2 == nil {
		return nil, errNoIdleConn
	}
	popConnFn := func() error {
		var err error
		if msWaitTime <= 0 {
			select {
			case conn = <-chan1:
			case conn = <-chan2:
			default:
				err = errNoIdleConn
			}
		} else {
			select {
			case conn = <-chan1:
			case conn = <-chan2:
			case <-time.After(time.Millisecond * time.Duration(msWaitTime)):
				err = errNoIdleConn
			}
		}
		if err != nil {
			return err
		}
		if conn == nil {
			this.putConn(conn)
			err = fmt.Errorf("%s, %s", descErrGetConnect, "conn is nil")
		} else {
			if utils.UnixTime()-conn.activeTime > pingInterval {
				if err = conn.Ping(); err != nil {
					this.putConn(conn)
					err = fmt.Errorf("%s, %s", descErrGetConnect, "ping err")
				}
			}
		}
		return err
	}
	if !utils.RetryExecute(popConnFn, tryTimes) {
		err = errNoIdleConn
	}
	return conn, err
}

func (this *Db) popConnFromConnChan(connChan <-chan *Conn, tryTimes, msWaitTime int) (conn *Conn) {
	if connChan == nil {
		return nil
	}
	popConnFn := func() error {
		var err error
		select {
		case conn = <-connChan:
			if conn == nil {
				this.putConn(conn)
				err = fmt.Errorf("%s, %s", descErrGetConnect, "conn is nil")
			} else {
				if utils.UnixTime()-conn.activeTime > pingInterval {
					if err = conn.Ping(); err != nil {
						this.putConn(conn)
						err = fmt.Errorf("%s, %s", descErrGetConnect, "ping err")
					}
				}
			}
		default:
			err = fmt.Errorf("%s, %s", descErrGetConnect, "no idle conn")
		}
		return err
	}
	utils.RetryExecuteWithWait(popConnFn, tryTimes, msWaitTime)
	return conn
}

func (this *Db) PutConn(conn *Conn) {
	this.putConn(conn)
}

func (this *Db) putConn(conn *Conn) {
	if this.status == DbStatusDown {
		return
	}
	if conn == nil {
		this.closeWorkConn(conn)
	} else {
		initConnChan := this.getInitConnChan()
		if initConnChan != nil && len(initConnChan) < this.initConnNum {
			initConnChan <- conn
		} else {
			extraConnChan := this.getExtraConnChan()
			if extraConnChan != nil {
				extraConnChan <- conn
			}
		}
	}
}

func (this *Db) getInitConnChan() chan *Conn {
	this.chanRWMutex.RLock()
	c := this.initConnChan
	defer this.chanRWMutex.RUnlock()
	return c
}

func (this *Db) getExtraConnChan() chan *Conn {
	this.chanRWMutex.RLock()
	c := this.extraConnChan
	this.chanRWMutex.RUnlock()
	return c
}

func (this *Db) getCheckConn() *Conn {
	this.chanRWMutex.RLock()
	checkConn := this.checkConn
	this.chanRWMutex.RUnlock()
	return checkConn
}

func (this *Db) closeWorkConn(conn *Conn) {
	this.connCreateMutex.Lock()
	defer this.connCreateMutex.Unlock()
	if conn != nil {
		conn.Close()
	}
	atomic.AddInt32(&this.curWorkConnNum, -1)
}

func (this *Db) ProcCmdMsg(msg *protocol.Msg) (replyMsg *protocol.Msg, err error) {
	var conn *Conn
	if conn, err = this.GetConnect(); err == nil {
		if err = msg.WriteMsg(conn.bw); err == nil {
			replyMsg, err = protocol.NewMsgFromReader(conn.br)
		}
		this.putConn(conn)
	} else {
		return nil, err
	}
	return
}

func (this *Db) ProcMultiCmdMsg(msgList []*protocol.Msg) (replyMsgList []*protocol.Msg, err error) {
	msgLen := len(msgList)
	replyMsgList = make([]*protocol.Msg, msgLen)
	if msgLen == 0 {
		return
	}
	var (
		conn     *Conn
		replyMsg *protocol.Msg
	)
	if conn, err = this.GetConnect(); err == nil {
		for _, msg := range msgList {
			if err = msg.WriteMsg(conn.bw); err == nil {
				if replyMsg, err = protocol.NewMsgFromReader(conn.br); err == nil {
					replyMsgList = append(replyMsgList, replyMsg)
				} else {
					replyMsgList = append(replyMsgList, nil)
				}
			}
		}
		this.putConn(conn)
	}
	return
}

func (this *Db) CloseDb() {
	if this.status == DbStatusDown {
		return
	}
	this.status = DbStatusDown
	initChan, extraChan := this.getInitConnChan(), this.getExtraConnChan()
	this.chanRWMutex.Lock()
	this.initConnChan = nil
	this.extraConnChan = nil
	this.curWorkConnNum = 0
	this.chanRWMutex.Unlock()
	close(initChan)
	for conn := range initChan {
		if conn != nil {
			conn.Close()
		}
	}
	close(extraChan)
	for conn := range extraChan {
		if conn != nil {
			conn.Close()
		}
	}
	this.closeCheckConn()
	fmt.Printf("[close db] addr:%s, db is down\n", this.addr)
}

func (this *Db) IsMaster() bool {
	return this.role == roleMaster
}

func (this *Db) String() string {
	return this.role + ", " + this.addr
}
