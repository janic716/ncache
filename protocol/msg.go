package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"ncache/utils"
	"ncache/utils/object"
	"strings"
)

type MsgType byte

var (
	OK = "OK"
)

var (
	MsgOK            = NewSimpleStringMsg("OK")
	MsgPING          = NewSimpleStringMsg("PING")
	MsgPONG          = NewSimpleStringMsg("PONG")
	MsgClusterSllots = NewArrayMsgFormStrings([]string{"CLUSTER", "SLOTS"})
	MsgReadOnly      = NewArrayMsgFormStrings([]string{"READONLY"})
	MsgAsking        = NewArrayMsgFormStrings([]string{"ASKING"})
	MsgReadWrite     = NewArrayMsgFormStrings([]string{"READWRITE"})
	MsgBackProcErr   = NewErrorMsg("ERR Backend process error")
	NullBulkString   = NewBulkStringMsg(nil)
)

var (
	ErrorUnknowMessageType = errors.New("Protocol: Unknow Message Type")
	ErrorBadCRLREnd        = errors.New("Protocol: bad CRLF end")
	ErrorBadArrayLen       = errors.New("Protocol: bad array len")
	ErrorBadBulkMsgLen     = errors.New("Protocol: bad Bulk Message len")
	ErrorUncompletedMsg    = errors.New("Protocol: uncomplete Message")
	ErrorMsgTooShort       = errors.New("Protocol: read insufficient length msg")
	ErrorMsgParametersLen  = errors.New("Protocol: worong request parameters length")
	ErrorNilMsg            = errors.New("Protocol: msg is nil")
)

var (
	ErrorMsgTypeFormat = "Protocol: Wrong Message Type! Need Message Type %s"

	msgPool *object.Opool
)

type Msg struct {
	mtype MsgType
	value []byte
	array []*Msg
}

func init() {
	//msgPool = object.NewOpool(4, func() interface{} {
	//	return &Msg{}
	//})
}

func getMsg(mtype MsgType) *Msg {
	//msg := msgPool.Get().(*Msg)
	//if msg != nil {
	//	msg.mtype = mtype
	//	return msg
	//}
	return &Msg{mtype: mtype}
}

func PutMsg(msg *Msg) {
	//if msg != nil {
	//	msg.value = nil
	//	msg.array = nil
	//	msgPool.Put(msg)
	//}
}

func NewMsgFromReader(br io.Reader) (*Msg, error) {
	if bufr, ok := br.(*bufio.Reader); ok {
		return readMsgFromReader(bufr)
	} else {
		return readMsgFromReader(bufio.NewReader(br))
	}
}

func NewFromBytes(b []byte) (*Msg, error) {
	r := bytes.NewReader(b)
	return NewMsgFromReader(r)
}

func (m *Msg) IsSimpleString() bool {
	return m.mtype == t_simple_string
}

func (m *Msg) IsError() bool {
	return m.mtype == t_error
}

func (m *Msg) IsInt() bool {
	return m.mtype == t_integer
}

func (m *Msg) IsBulk() bool {
	return m.mtype == t_bulk_string
}

func (m *Msg) IsArray() bool {
	return m.mtype == t_array
}

//todo:
func NewSimpleStringMsg(value string) (msg *Msg) {
	msg = &Msg{
		mtype: t_simple_string,
		value: []byte(value),
	}
	return
}

func NewOkMsg() *Msg {
	return MsgOK
}

//todo:
func NewIntegerMsg(value int64) (msg *Msg) {
	msg = &Msg{
		mtype: t_integer,
		value: []byte(utils.Int64ToString(value)),
	}
	return
}

//todo:
func NewBulkStringMsg(value []byte) (msg *Msg) {
	msg = &Msg{
		mtype: t_bulk_string,
		value: value,
	}
	return
}

//todo:
func NewArrayMsg(array []*Msg) (msg *Msg) {
	msg = &Msg{
		mtype: t_array,
		array: array,
	}
	return
}

//todo:
func NewErrorMsg(errInfo string) (msg *Msg) {
	msg = &Msg{
		mtype: t_error,
		value: []byte(errInfo),
	}
	return
}

func NewErrorMsgFmt(format string, a ...interface{}) (msg *Msg) {
	s := utils.GetStringFmt(format, a)
	msg = &Msg{
		mtype: t_error,
		value: []byte(s),
	}
	return
}

func (this *Msg) WriteMsg(writer io.Writer) error {
	var (
		bw *bufio.Writer
		ok bool
	)
	if bw, ok = writer.(*bufio.Writer); !ok {
		bw = bufio.NewWriter(writer)
	}
	err := writeMsg(bw, this)
	bw.Flush()
	return err
}

func (this *Msg) WriteToBytes() ([]byte, error) {
	var b = &bytes.Buffer{}
	if err := this.WriteMsg(bufio.NewWriter(b)); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func errorWrongMsgType(mtype MsgType) error {
	switch mtype {
	case t_simple_string:
		return fmt.Errorf(ErrorMsgTypeFormat, "simple string")
	case t_error:
		return fmt.Errorf(ErrorMsgTypeFormat, "error")
	case t_integer:
		return fmt.Errorf(ErrorMsgTypeFormat, "integer")
	case t_bulk_string:
		return fmt.Errorf(ErrorMsgTypeFormat, "bulk string")
	case t_array:
		return fmt.Errorf(ErrorMsgTypeFormat, "array")
	default:
		return ErrorUnknowMessageType
	}
}

func (this *Msg) GetInt() int64 {
	n, _ := utils.BytesToInt64(this.value)
	return n
}

func (this *Msg) GetSimpleString() (string, error) {
	if !this.IsSimpleString() {
		return "", errorWrongMsgType(t_simple_string)
	}
	if this.value == nil {
		return "", ErrorUncompletedMsg
	}
	return string(this.value), nil
}

func (this *Msg) GetError() (string, error) {
	if !this.IsError() {
		return "", errorWrongMsgType(t_error)
	}
	if this.value == nil {
		return "", ErrorUncompletedMsg
	}
	return string(this.value), nil
}

func (this *Msg) GetStr() (string, error) {
	if this == nil {
		return "", ErrorNilMsg
	}
	if this.IsBulk() || this.IsSimpleString() || this.IsError() {
		return string(this.value), nil
	} else if this.IsInt() {
		val := this.GetInt()
		return fmt.Sprint(val), nil
	} else if this.IsError() {
		return this.GetError()
	}
	return "", ErrorUncompletedMsg
}

func (this *Msg) GetArray() []*Msg {
	return this.array
}

func (this *Msg) GetArrayLen() int {
	return len(this.array)
}

func (this *Msg) GetValueBytes() ([]byte, error) {
	if (this.mtype == t_error ||
		this.mtype == t_simple_string ||
		this.mtype == t_integer) && this.value == nil {
		return nil, ErrorUncompletedMsg
	}
	return this.value, nil
}

func (this *Msg) IsBulkStringArray() bool {
	if !this.IsArray() {
		return false
	}
	for _, v := range this.array {
		if !v.IsBulk() {
			return false
		}
	}
	return true
}

func NewArrayMsgFormStrings(bs []string) *Msg {
	msg := &Msg{
		mtype: t_array,
	}
	for _, b := range bs {
		msg.array = append(msg.array, NewBulkStringMsg([]byte(b)))
	}
	return msg
}

func NewCmdMsg(cmd string) *Msg {
	msg := &Msg{
		mtype: t_array,
	}
	list := strings.Split(cmd, " ")
	for _, v := range list {
		v = strings.TrimSpace(v)
		if len(v) > 0 {
			msg.array = append(msg.array, NewBulkStringMsg([]byte(v)))
		}
	}
	return msg
}

func (this *Msg) Args() ([]string, error) {
	if !this.IsBulkStringArray() {
		return nil, errorWrongMsgType(t_array)
	}
	if this.array == nil {
		return nil, ErrorMsgParametersLen
	}
	var args []string
	for _, m := range this.array {
		args = append(args, string(m.value))
	}
	return args, nil
}

func (this *Msg) ArgsBytes() (args [][]byte) {
	for _, m := range this.array {
		args = append(args, m.value)
	}
	return args
}

// Bulk array Append Message
func (this *Msg) AppendMsg(msg *Msg) *Msg {
	this.array = append(this.array, msg)
	return this
}

func (this *Msg) String() string {
	switch this.mtype {
	case t_simple_string:
		return "(string) " + string(this.value)
	case t_error:
		return "(error) " + string(this.value)
	case t_integer:
		return "(int) " + string(this.value)
	case t_bulk_string:
		if this.value == nil {
			return "(bulk) nil"
		}
		return "(bulk) " + string(this.value)
	case t_array:
		a := make([]string, 0)
		for i, v := range this.array {
			if v.value == nil {
				a = append(a, utils.Int64ToString(int64(i))+") "+"nil")
				continue
			}
			a = append(a, utils.Int64ToString(int64(i))+") "+string(v.value))
		}
		return strings.Join(a, "\n")
	default:
		return "(unknown)"
	}
}

func NewErrMsgFormat(format string, a ...interface{}) *Msg {
	s := fmt.Sprintf(format, a...)
	return NewErrorMsg(s)
}
