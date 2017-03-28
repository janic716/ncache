package utils

import (
	"fmt"
	"reflect"
	"time"
)

func TestFunc(f func(), times int) (info string) {
	start := time.Now().UnixNano()
	for i := 0; i < times; i++ {
		f()
	}
	end := time.Now().UnixNano()
	totalTime := float64(end-start) / 1000
	res := fmt.Sprintf("execute-times:%d | total-time:%.3f us | avg-time:%.3f us ", times, totalTime, float64(totalTime)/float64(times))
	return res
}

func AssertMust(b bool) {
	if b {
		return
	}
	panic("assertion failed")
}

func AssertMustNoError(err error) {
	if err == nil {
		return
	}
	panic(fmt.Sprintf("%s error happens, assertion failed", err.Error()))
}

func DeepEqual(x, y interface{}) bool {
	return reflect.DeepEqual(x, y)
}
