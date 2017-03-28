package protocol

import (
	"bytes"
	"strconv"
	"testing"

	"ncache/utils"
)

func TestDecodeInvalidRequests(t *testing.T) {
	test := []string{
		"*hello\r\n",
		"*-100\r\n",
		"*3\r\nhi",
		"*3\r\nhi\r\n",
		"*4\r\n$1",
		"*4\r\n$1\r",
		"*4\r\n$1\n",
		"*2\r\n$3\r\nget\r\n$what?\r\nx\r\n",
		"*4\r\n$3\r\nget\r\n$1\r\nx\r\n",
		"*2\r\n$3\r\nget\r\n$1\r\nx",
		"*2\r\n$3\r\nget\r\n$1\r\nx\r",
		"*2\r\n$3\r\nget\r\n$100\r\nx\r\n",
		"$6\r\nfoobar\r",
		"$0\rn\r\n",
		"$-1\n",
		"*0",
		"*2n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"*-\r\n",
		"+OK\n",
		"-Error message\r",
	}
	for _, s := range test {
		_, err := NewFromBytes([]byte(s))
		utils.AssertMust(err != nil)
	}
}

func TestReadFromBytes(t *testing.T) {
	_, err := NewFromBytes([]byte("\r\n"))
	utils.AssertMust(err != nil)
}

func TestReadBulkBytes(t *testing.T) {
	test := "*2\r\n$5\r\nHELLO\r\n$5\r\nWorld\r\n"
	msg, err := NewFromBytes([]byte(test))
	utils.AssertMustNoError(err)
	utils.AssertMust(len(msg.array) == 2)
	m1 := msg.array[0]
	utils.AssertMust(bytes.Equal(m1.value, []byte("HELLO")))
	m2 := msg.array[1]
	utils.AssertMust(bytes.Equal(m2.value, []byte("World")))
}

func TestReadMsg(t *testing.T) {
	test := []string{
		"$6\r\nfoobar\r\n",
		"$0\r\n\r\n",
		"$-1\r\n",
		"*0\r\n",
		"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
		"*3\r\n:1\r\n:2\r\n:3\r\n",
		"*-1\r\n",
		"+OK\r\n",
		"-Error message\r\n",
		"*2\r\n$1\r\n0\r\n*0\r\n",
		"*3\r\n$4\r\nEVAL\r\n$31\r\nreturn {1,2,{3,'Hello World!'}}\r\n$1\r\n0\r\n",
	}
	for _, s := range test {
		_, err := NewFromBytes([]byte(s))
		utils.AssertMustNoError(err)
	}
}

func TestNewSimpleStringMsg(t *testing.T) {
	msg := NewSimpleStringMsg("OK")
	testEncodeAndCheck(t, msg, []byte("+OK\r\n"))
}

func TestNewErrorMsg(t *testing.T) {
	msg := NewErrorMsg("Err some error")
	testEncodeAndCheck(t, msg, []byte("-Err some error\r\n"))
}

func TestNewIntegerMsg(t *testing.T) {
	for _, v := range []int{-2, -1, 0, 1, 2, 5, 10, 100, 1000} {
		s := strconv.Itoa(v)
		msg := NewIntegerMsg(int64(v))
		testEncodeAndCheck(t, msg, []byte(":"+s+"\r\n"))
	}
}

func TestNewBulkStringMsg(t *testing.T) {
	msg := NewBulkStringMsg(nil)
	testEncodeAndCheck(t, msg, []byte("$-1\r\n"))
	msg.value = []byte{}
	testEncodeAndCheck(t, msg, []byte("$0\r\n\r\n"))
	msg.value = []byte("This is a test")
	testEncodeAndCheck(t, msg, []byte("$14\r\nThis is a test\r\n"))
}

func TestNewArrayMsg(t *testing.T) {
	msg := NewArrayMsg(nil)
	testEncodeAndCheck(t, msg, []byte("*-1\r\n"))
	msg.array = []*Msg{}
	testEncodeAndCheck(t, msg, []byte("*0\r\n"))
	msg.array = append(msg.array, NewIntegerMsg(2017))
	testEncodeAndCheck(t, msg, []byte("*1\r\n:2017\r\n"))
	msg.array = append(msg.array, NewBulkStringMsg(nil))
	testEncodeAndCheck(t, msg, []byte("*2\r\n:2017\r\n$-1\r\n"))
	msg.array = append(msg.array, NewBulkStringMsg([]byte("This is a test")))
	testEncodeAndCheck(t, msg, []byte("*3\r\n:2017\r\n$-1\r\n$14\r\nThis is a test\r\n"))
}

func testEncodeAndCheck(t *testing.T, msg *Msg, expect []byte) {
	b, err := msg.WriteToBytes()
	utils.AssertMustNoError(err)
	utils.AssertMust(bytes.Equal(b, expect))
}

func TestMsg_GetInt(t *testing.T) {
	var i int64 = -32768
	for ; i < 32768; i++ {
		msg := NewIntegerMsg(i)
		ret, err := msg.GetInt()
		utils.AssertMustNoError(err)
		utils.AssertMust(ret == i)
	}
}

func TestMsg_GetString(t *testing.T) {
	strings := []string{
		"OK",
		"PING",
		"PONG",
		"Err This is error Test",
		"",
		"Test",
	}
	for _, s := range strings {
		msg := NewSimpleStringMsg(s)
		ret, err := msg.GetSimpleString()
		utils.AssertMustNoError(err)
		utils.AssertMust(s == ret)
	}
}
