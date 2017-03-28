package object

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Opool struct {
	pools    []*sync.Pool
	getIndex int64
	putIndex int64
	poolNum  int
}

func NewOpool(poolNum int, new func() interface{}) *Opool {
	if poolNum < 1 {
		poolNum = 1
	}
	opool := &Opool{poolNum: poolNum}
	opool.pools = make([]*sync.Pool, poolNum)
	for i := 0; i < poolNum; i++ {
		opool.pools[i] = &sync.Pool{New: new}
	}
	return opool
}

func (this *Opool) Get() interface{} {
	i := atomic.AddInt64(&this.getIndex, 1)
	return this.pools[i%int64(this.poolNum)].Get()
}

func (this *Opool) Put(x interface{}) {
	i := atomic.AddInt64(&this.getIndex, 1)
	this.pools[i%int64(this.poolNum)].Put(x)
}

func (this *Opool) Stat() string {
	return fmt.Sprintf("get count: %d, put count: %d\n", this.getIndex, this.putIndex)
}
