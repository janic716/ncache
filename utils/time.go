package utils

import (
	"time"
	"math/rand"
)

var (
	currentTime int64
	nowTime     time.Time
)

func init() {
	nowTime = time.Now()
	currentTime = nowTime.Unix()
	rand.Seed(currentTime)
	go tick()
}

//使用频率最高, 采用弱精度时间
func UnixTime() int64 {
	return currentTime
}

//精确到秒
func NowTime() time.Time {
	return nowTime
}

func UnixTimeNano() int64 {
	return time.Now()   .UnixNano()
}

func UnixTimeMicro() int64 {
	return time.Now().UnixNano() / 1000
}

func tick() {
	for {
		select {
		case <-time.Tick(time.Second):
			currentTime = time.Now().Unix()
			nowTime = time.Now()
			rand.Seed(currentTime)
		}
	}
}

func FuncCost(f func()) int64 {
	start := time.Now().UnixNano()
	f()
	return (time.Now().UnixNano() - start) / 1000
}

func TimerTask(task func() error, interval time.Duration) {
	defer RecoverFunc(task, true)
	for {
		select {
		case <-time.Tick(interval):
			if task == nil {
				break
			}
			if err := task(); err != nil {
				return
			}
		}
	}
}
