package protocol

import (
	"bufio"
	"io"

	"github.com/pkg/errors"
	"ncache/utils"
)

const (
	t_simple_string MsgType = '+' //2b -- 43
	t_error         MsgType = '-' //2d -- 45
	t_integer       MsgType = ':' //3a -- 58
	t_bulk_string   MsgType = '$' //24 -- 36
	t_array         MsgType = '*' //2a -- 42

	cr_suffix = byte('\r')
	lf_suffix = byte('\n')
)

var (
	crlf = []byte{cr_suffix, lf_suffix}
)

func readMsgFromReader(br *bufio.Reader) (*Msg, error) {
	prefix, err := br.ReadByte()
	if err != nil {
		return nil, err
	}
	switch MsgType(prefix) {
	case t_simple_string:
		return readSimpleString(br)
	case t_integer:
		return readInteger(br)
	case t_bulk_string:
		return readBulkStrings(br)
	case t_array:
		return readArray(br)
	case t_error:
		return readError(br)
	}
	return nil, ErrorUnknowMessageType
}

func readToCRLF(bufReader *bufio.Reader) ([]byte, error) {
	var (
		buf []byte
		err error
	)
	if buf, err = bufReader.ReadBytes(lf_suffix); err != nil {
		return nil, err
	}

	n := len(buf) - 2
	if n < 0 {
		return nil, ErrorMsgTooShort
	}
	if buf[n] != cr_suffix {
		return nil, ErrorBadCRLREnd
	}
	return buf[:n], nil
}

func readGeneric(br *bufio.Reader, mtype MsgType) (msg *Msg, err error) {
	msg = getMsg(mtype)
	var buf []byte
	if buf, err = readToCRLF(br); err != nil {
		return nil, err
	}
	msg.value = buf
	return
}

func readSimpleString(br *bufio.Reader) (*Msg, error) {
	return readGeneric(br, t_simple_string)
}

func readError(br *bufio.Reader) (*Msg, error) {
	return readGeneric(br, t_error)
}

func readInteger(br *bufio.Reader) (*Msg, error) {
	return readGeneric(br, t_integer)
}

func readBulkStrings(br *bufio.Reader) (msg *Msg, err error) {
	msg = getMsg(t_bulk_string)
	var (
		buf      []byte
		valueLen int64
	)
	if buf, err = readToCRLF(br); err != nil {
		return nil, err
	}
	if valueLen, err = utils.BytesToInt64(buf); err != nil {
		return
	}

	switch {
	case valueLen < -1:
		return nil, ErrorBadBulkMsgLen
	case valueLen == -1:
		msg.value = nil
		return
	}

	buf = make([]byte, int(valueLen)+2)
	if _, err = io.ReadFull(br, buf); err != nil {
		return nil, err
	}
	if buf[valueLen] != '\r' || buf[valueLen+1] != '\n' {
		return nil, ErrorBadCRLREnd
	}
	msg.value = buf[:valueLen]
	return
}

func readArray(br *bufio.Reader) (msg *Msg, err error) {
	msg = getMsg(t_array)
	var (
		buf      []byte
		msgLen   int64
		msgArray []*Msg
	)
	if buf, err = readToCRLF(br); err != nil {
		return nil, err
	}
	if msgLen, err = utils.BytesToInt64(buf); err != nil {
		return nil, err
	}
	switch {
	case msgLen < -1:
		return nil, ErrorBadArrayLen
	case msgLen == -1:
		msg.array = nil
		return
	}
	msgArray = make([]*Msg, msgLen)
	var i int64 = 0
	for ; i < msgLen; i++ {
		if m, err := readMsgFromReader(br); err != nil {
			return nil, err
		} else {
			msgArray[i] = m
		}
	}
	msg.array = msgArray
	return
}

func writeMsg(bw *bufio.Writer, msg *Msg) error {
	var err error
	if err = bw.WriteByte(byte(msg.mtype)); err != nil {
		return err
	}
	switch msg.mtype {
	case t_simple_string, t_integer, t_error:
		err = writeBytesWithCRLF(bw, msg.value)
	case t_bulk_string:
		err = writeBulkString(bw, msg.value)
	case t_array:
		err = writeArray(bw, msg.array)
	}
	return err
}

func writeInt(bw *bufio.Writer, i int64) error {
	return writeBytesWithCRLF(bw, []byte(utils.Int64ToString(i)))
}

func writeBytesWithCRLF(bw *bufio.Writer, value []byte) error {
	var err error
	if _, err = bw.Write(value); err != nil {
		return err
	}
	if _, err = bw.Write(crlf); err != nil {
		return err
	}
	return nil
}

func writeBulkString(bw *bufio.Writer, value []byte) error {
	if value == nil {
		return writeInt(bw, -1)
	} else {
		if err := writeInt(bw, int64(len(value))); err != nil {
			return err
		}
		return writeBytesWithCRLF(bw, value)
	}
}

func writeArray(bw *bufio.Writer, array []*Msg) error {
	if array == nil {
		return writeInt(bw, -1)
	} else {
		if err := writeInt(bw, int64(len(array))); err != nil {
			return err
		}
		for _, msg := range array {
			if err := writeMsg(bw, msg); err != nil {
				return err
			}
		}
		return nil
	}
}

func Ping(reader io.Reader, writer io.Writer) error {
	err := NewCmdMsg("PING").WriteMsg(writer)
	if err == nil {
		var replyMsg *Msg
		if replyMsg, err = NewMsgFromReader(reader); err == nil {
			var str string
			str, err = replyMsg.GetSimpleString()
			if str != "PONG" {
				err = errors.New("ping err")
			}
			//PutMsg(replyMsg)
		}
	}
	return err
}

func IsOkMsg(msg *Msg) bool {
	return msg != nil && msg.mtype == t_simple_string && string(msg.value) == OK
}
