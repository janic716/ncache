package utils

import (
	"github.com/janic716/golib/log"
	"reflect"
	"runtime/debug"
	"time"
)

//执行指定函数f, 如果发生错误, 则重试
func RetryExecute(f func() error, retryTimes int) bool {
	return RetryExecuteWithWait(f, retryTimes, 0)
}

func RetryExecuteWithWait(f func() error, retryTimes int, msWaitTime int) bool {
	if retryTimes <= 0 {
		return false
	}
	var err error
	for i := 0; i < retryTimes; i++ {
		if err = f(); err == nil {
			return true
		}
		if msWaitTime > 0 {
			time.Sleep(time.Millisecond * time.Duration(msWaitTime))
		}
	}
	return false
}

func RetryExecuteWithTimeout(f func() error, retryTimes int, msTimeout int) bool {
	job := make(chan bool)
	go func() {
		job <- RetryExecuteWithWait(f, retryTimes, 0)
	}()
	select {
	case <-time.After(time.Millisecond * time.Duration(msTimeout)):
		return false
	case <-job:
		return true
	}
}

func Execute(f func() error, times int) int {
	var (
		err error
		res int
	)
	for i := 0; i < times; i++ {
		if err = f(); err == nil {
			res++
		}
	}
	return res
}

func RecoverFunc(p interface{}, logStack bool) {
	if r := recover(); r != nil {
		t := reflect.TypeOf(p)
		if logStack {
			log.Errorf("%s panic, stack: %s", t.String(), string(debug.Stack()))
		} else {
			log.Errorf("%s panic", t.String())
		}
	}
}
