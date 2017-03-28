package stress

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/spf13/viper"
	"ncache/tool/common"
	"ncache/utils"
)

var seq int64
var keyToUse string
var keysToUse []string
var value string

var f *os.File

func init() {
	var (
		err error
	)
	currentPath := common.GetCurrentPath()
	tempFileName := "result" + "_" + strconv.FormatInt(time.Now().Unix(), 10)
	name := currentPath + "/" + tempFileName
	if !utils.IsValidFile(name) {
		utils.FileRemove(name)

	}
	f, err = os.Create(name)
	if err != nil {
		fmt.Printf("Creat File Failed, error info: %s\n", err.Error())
		os.Exit(1)
	}
}

func WriteRecord(r *Record) {
	for _, v := range r.r {
		f.WriteString(v + "\n")
	}
	f.WriteString("\n\n\n\n")
	f.Sync()
}

type Record struct {
	r []string
}

func (r *Record) insert(s string) {
	s = time.Now().String() + "  " + s
	r.r = append(r.r, s)
}

type Ret struct {
	t time.Duration
	d int
	c int
}

func initKeyValues(preFix string, valueSize int) {
	prefixs := strings.Split(preFix, ",")
	keysToUse = keysToUse[0:0]
	for _, prefix := range prefixs {
		prefix = strings.ToLower(prefix)
		keysToUse = append(keysToUse, prefix+":Test")
	}
	if len(keysToUse) == 0 {
		keysToUse = append(keysToUse, "default:Test")
	}

	keyToUse = "default:Test"

	var buf []byte

	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(os.Getegid())))

	for i := 0; i < valueSize; i++ {
		buf = append(buf, byte(r.Int31n(26)+65))
	}
	value = string(buf)
}

func goClientPorcess(method, addr string, rounds int, varPrefix, persistent bool, wg *sync.WaitGroup, ch chan<- Ret) {
	wg.Add(1)
	defer wg.Done()
	var ret Ret
	defer func() { ch <- ret }()

	elapseHelpFunc := func(t time.Time) time.Duration { return time.Now().Sub(t) }

	start := time.Now()

	if persistent {
		conn, err := redis.Dial("tcp", addr)
		if err != nil {
			ret.t = elapseHelpFunc(start)
			return
		}
		for i := 0; i < rounds; i++ {
			key := keyToUse
			if varPrefix {
				key = keysToUse[i%len(keysToUse)]
			}
			method = strings.ToUpper(method)
			switch method {
			case "SET":
				_, err = conn.Do("SET", key, value)
			case "GET":
				_, err = conn.Do("GET", key)
			}
			if err != nil {
				ret.t = elapseHelpFunc(start)
				ret.c = ret.c + 1
				return
			}
			ret.d = i
			atomic.AddInt64(&seq, 1)
		}
		ret.t = elapseHelpFunc(start)
	} else {
		//  Not Support Now
	}
	return
}

func recordHelpFunc(r *Record, exit chan bool) {
	atomic.SwapInt64(&seq, 0)
	var oldValue int64
	oldValue = 0
	for {
		select {
		case <-exit:
			return
		default:
			time.Sleep(time.Second * 1)
		}
		nowValue := atomic.LoadInt64(&seq)
		r.insert(utils.GetStringFmt("Recording: %d %d %d", oldValue, nowValue, nowValue-oldValue))
		oldValue = nowValue
	}
}

func Stress(cnums int, persistent bool) {
	atomic.SwapInt64(&seq, 0)
	r := &Record{}
	defer WriteRecord(r)

	r.insert("Start sterss testing")
	r.insert("parameters: ")
	addr := viper.GetString("address")
	r.insert("address:" + addr)
	port := viper.GetString("port")
	r.insert("port: " + port)
	address := addr + ":" + port
	r.insert(utils.GetStringFmt("Client nums: %d", cnums))
	rounds := viper.GetInt("rounds")
	r.insert(utils.GetStringFmt("Per client %d rounds", rounds))
	prefixs := viper.GetString("prefix")
	r.insert("key prefix: " + prefixs)
	valueLen := viper.GetInt("value_size")
	r.insert("value lenght: " + strconv.Itoa(valueLen))
	method := viper.GetString("method")
	if method == "" {
		method = "both"
	}
	method = strings.ToUpper(method)
	r.insert("test method: " + method)

	varPrefix := viper.GetBool("var_prefix")
	r.insert(utils.GetStringFmt("Var prefix: %t", varPrefix))

	// init Keys
	initKeyValues(prefixs, valueLen)

	if method == "BOTH" || method == "SET" {
		r.insert("Start testing SET")
		wg := &sync.WaitGroup{}
		ch := make(chan Ret, cnums)
		reExit1 := make(chan bool, 1)
		go recordHelpFunc(r, reExit1)
		time.Sleep(time.Millisecond * 10)
		for i := 0; i < cnums; i++ {
			go goClientPorcess("SET", address, rounds, varPrefix, persistent, wg, ch)
		}
		time.Sleep(time.Second * 1)
		wg.Wait()
		close(ch)
		reExit1 <- true
		done := 0
		crash := 0
		var maxDur, minDur time.Duration
		maxDur = time.Duration(int64(0))
		minDur = time.Duration(math.MaxInt64)
		for r := range ch {
			done += r.d
			crash += r.c
			maxDur = utils.MaxDuration(maxDur, r.t)
			minDur = utils.MinDuration(minDur, r.t)
		}
		r.insert(utils.GetStringFmt("total SET %d rounds", done))
		r.insert(utils.GetStringFmt("Client crash %d", crash))
		r.insert(utils.GetStringFmt("最长用时：%s", maxDur))
		r.insert(utils.GetStringFmt("最短用时：%s", minDur))

		time.Sleep(2 * time.Second)
	}

	if method == "BOTH" || method == "GET" {
		r.insert("Start testing GET")
		wg := &sync.WaitGroup{}
		ch := make(chan Ret, cnums)
		reExit2 := make(chan bool, 1)
		go recordHelpFunc(r, reExit2)
		time.Sleep(time.Millisecond * 10)
		for i := 0; i < cnums; i++ {
			go goClientPorcess("GET", address, rounds, varPrefix, persistent, wg, ch)
		}
		time.Sleep(time.Second * 1)
		wg.Wait()
		close(ch)
		reExit2 <- true
		done := 0
		crash := 0
		var maxDur, minDur time.Duration
		maxDur = time.Duration(int64(0))
		minDur = time.Duration(math.MaxInt64)
		for r := range ch {
			done += r.d
			crash += r.c
			maxDur = utils.MaxDuration(maxDur, r.t)
			minDur = utils.MinDuration(minDur, r.t)
		}
		r.insert(utils.GetStringFmt("total Get %d rounds", done))
		r.insert(utils.GetStringFmt("Client crash %d", crash))
		r.insert(utils.GetStringFmt("最长用时：%s", maxDur))
		r.insert(utils.GetStringFmt("最短用时： %s", minDur))
	}
}
