package filter

//todo: 根据命令过滤
import (
	"errors"
	"ncache/protocol"
	"strings"
)

const (
	C_READ    = iota //cmd for read
	C_WRITE          //cmd for write
	C_ADMIN          //cmd for sys
	C_LOCAL          //cmd for process local
	C_UNKNOWN        //unknown cmd
)

var cmdMap = make(map[string]int)

func init() {
	cmdMap["PING"] = C_LOCAL

	cmdMap["CLUSTER"] = C_WRITE

	cmdMap["EXISTS"] = C_READ
	cmdMap["DEL"] = C_WRITE
	cmdMap["EXPIRE"] = C_WRITE
	cmdMap["EXPIREAT"] = C_WRITE
	cmdMap["PEXPIRE"] = C_WRITE
	cmdMap["PEXPIREAT"] = C_WRITE
	cmdMap["TTL"] = C_READ
	cmdMap["PTTL"] = C_READ

	cmdMap["INCR"] = C_WRITE
	cmdMap["INCRBY"] = C_WRITE
	cmdMap["DECR"] = C_WRITE
	cmdMap["DECRBY"] = C_WRITE

	cmdMap["GET"] = C_READ
	cmdMap["MGET"] = C_READ
	cmdMap["SET"] = C_WRITE
	cmdMap["SETNX"] = C_WRITE
	cmdMap["SETEX"] = C_WRITE
	cmdMap["PSETEX"] = C_WRITE
	cmdMap["MSET"] = C_WRITE
	cmdMap["MSETNX"] = C_WRITE

	cmdMap["SADD"] = C_WRITE
	cmdMap["SREM"] = C_WRITE
	cmdMap["SCARD"] = C_READ
	cmdMap["SMEMBERS"] = C_READ
	cmdMap["SISMEMBER"] = C_READ

	cmdMap["OADD"] = C_WRITE
	cmdMap["OREM"] = C_WRITE
	cmdMap["OREMRANGEBYRANK"] = C_WRITE
	cmdMap["ORANGE"] = C_READ
	cmdMap["OREVRANGE"] = C_READ
	cmdMap["ORANGEBYMEMBER"] = C_READ
	cmdMap["OREVRANGEBYMEMBER"] = C_READ
	cmdMap["OCARD"] = C_READ
	cmdMap["OGETMAXLEN"] = C_READ
	cmdMap["OGETFINITY"] = C_READ

	cmdMap["ZADD"] = C_WRITE
	cmdMap["ZREM"] = C_WRITE
	cmdMap["ZREMRANGEBYRANK"] = C_WRITE
	cmdMap["ZREMRANGEBYSCORE"] = C_WRITE
	cmdMap["ZINCRBY"] = C_WRITE
	cmdMap["ZRANGE"] = C_READ
	cmdMap["ZREVRANGE"] = C_READ
	cmdMap["ZRANGEBYSCORE"] = C_READ
	cmdMap["ZREVRANGEBYSCORE"] = C_READ
	cmdMap["ZSCORE"] = C_READ
	cmdMap["ZCARD"] = C_READ
	cmdMap["ZGETMAXLEN"] = C_READ
	cmdMap["ZGETFINITY"] = C_READ

	cmdMap["XADD"] = C_WRITE
	cmdMap["XREM"] = C_WRITE
	cmdMap["XREMRANGEBYRANK"] = C_WRITE
	cmdMap["XREMRANGEBYSCORE"] = C_WRITE
	cmdMap["XINCRBY"] = C_WRITE
	cmdMap["XRANGE"] = C_READ
	cmdMap["XREVRANGE"] = C_READ
	cmdMap["XRANGEBYSCORE"] = C_READ
	cmdMap["XREVRANGEBYSCORE"] = C_READ
	cmdMap["XSCORE"] = C_READ
	cmdMap["XCARD"] = C_READ
	cmdMap["XGETMAXLEN"] = C_READ
	cmdMap["XGETFINITY"] = C_READ

	cmdMap["HGET"] = C_READ
	cmdMap["HMGET"] = C_READ
	cmdMap["HSET"] = C_WRITE
	cmdMap["HSETNX"] = C_WRITE
	cmdMap["HMSET"] = C_WRITE
	cmdMap["HDEL"] = C_WRITE
	cmdMap["HEXISTS"] = C_READ
	cmdMap["HSTRLEN"] = C_READ
	cmdMap["HINCRBY"] = C_WRITE
	cmdMap["HLEN"] = C_READ
	cmdMap["HKEYS"] = C_READ
	cmdMap["HVALS"] = C_READ
	cmdMap["HGETALL"] = C_READ

	cmdMap["LPUSH"] = C_WRITE
	cmdMap["LPUSHX"] = C_WRITE
	cmdMap["RPUSH"] = C_WRITE
	cmdMap["RPUSHX"] = C_WRITE
	cmdMap["LPOP"] = C_WRITE
	cmdMap["RPOP"] = C_WRITE
	cmdMap["LREM"] = C_WRITE
	cmdMap["LTRIM"] = C_WRITE
	cmdMap["LSET"] = C_WRITE
	cmdMap["LINDEX"] = C_READ
	cmdMap["LRANGE"] = C_READ
	cmdMap["LLEN"] = C_READ
	cmdMap["ROUTER"] = C_ADMIN
}

func IsValidCmd(cmd string) bool {
	_, ok := cmdMap[cmd]
	return ok
}

func IsWriteCmd(cmd string) bool {
	return checkCmd(cmd, C_WRITE)
}

func IsReadCmd(cmd string) bool {
	return checkCmd(cmd, C_READ)
}

func IsAdminCmd(cmd string) bool {
	return checkCmd(cmd, C_ADMIN)
}

func checkCmd(cmd string, cmdType int) bool {
	t, ok := cmdMap[strings.ToUpper(cmd)]
	return ok && t == cmdType
}

func getCmdFromMsg(msg *protocol.Msg) (cmd string, err error) {
	var args []string
	if args, err = msg.Args(); err != nil {
		return "", errors.New("invalid cmd msg")
	}
	if len(args) > 0 {
		cmd = strings.ToUpper(args[0])
	}
	return
}

func checkCmdMsg(msg *protocol.Msg, cmdType int) bool {
	if cmd, err := getCmdFromMsg(msg); err != nil {
		return false
	} else {
		return checkCmd(cmd, cmdType)
	}
}
func IsWriteCmdMsg(msg *protocol.Msg) bool {
	return checkCmdMsg(msg, C_WRITE)
}

func IsReadCmdMsg(msg *protocol.Msg) bool {
	return checkCmdMsg(msg, C_READ)
}
