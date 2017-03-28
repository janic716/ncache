package main

import (
	"runtime"
)
import (
	"flag"
	"fmt"
	"github.com/spf13/viper"
	"ncache/tool/common"
	"ncache/tool/stress"
)

func main() {
	var addr, port, prefix, method string
	var rounds, valueLen int
	var varPrefix bool
	flag.StringVar(&addr, "address", "127.0.0.1", "server address")
	flag.StringVar(&port, "port", "10000", "server port")
	flag.StringVar(&prefix, "prefix", "default", "key prefix")
	flag.StringVar(&method, "method", "", "test method")
	flag.IntVar(&rounds, "rounds", 10000, "test rounds")
	flag.IntVar(&valueLen, "value_len", 10, "value lenght")
	flag.BoolVar(&varPrefix, "var_prefix", false, "var prefix")
	var cpuCoreNums int
	flag.IntVar(&cpuCoreNums, "core_num", -1, "cpu core nums")
	var cnums int
	flag.IntVar(&cnums, "cnums", 0, "client nums")
	flag.Parse()

	if cpuCoreNums > runtime.NumCPU() {
		cpuCoreNums = runtime.NumCPU()
	}

	runtime.GOMAXPROCS(cpuCoreNums)

	viper.Set("address", addr)
	viper.Set("port", port)
	viper.Set("prefix", prefix)
	viper.Set("method", method)
	viper.Set("rounds", rounds)
	viper.Set("value_size", valueLen)

	stop := make(chan bool, 1)
	go common.StartProfile(stop)
	if cnums == 0 {
		numsSet := []int{5, 10, 20, 50, 100}
		for _, c := range numsSet {
			stress.Stress(c, true)
		}
	} else {
		stress.Stress(cnums, true)
	}

	fmt.Println("process end")
	stop <- true
}
