package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/janic716/golib/log"
	"ncache/backend/route"
	"ncache/config"
	. "ncache/server"
	"ncache/tool/common"
)

//todo:
func initConf() {
	if err := config.InitConfig(); err != nil {
		fmt.Printf("Init Conf error: %s\n", err.Error())
		os.Exit(1)
	}
}

//todo
func initLog() {
	conf, err := config.GetLogConf()
	if err != nil {
		fmt.Printf("Init Log error: %s\n", err.Error())
		os.Exit(1)
	}
	log.InitLogger(*conf)
}

func initBackend() {
	err := route.InitBackendMap()
	if err != nil {
		fmt.Printf("Init backend error:%s\n", err.Error())
		os.Exit(1)
	}
}

//todo:
func initServer() (*Server, error) {
	server, err := NewServer()
	if err != nil {
		return nil, err
	}
	return server, nil
}

//todo:
func initSignal() {

}

func initDebug() {
	if conf, err := config.GetServerConf(); err == nil {
		if conf.PprofEnable {
			go func() {
				if err := http.ListenAndServe(conf.PprofAddr, nil); err == nil {
					log.Notice("pprof start success, addr: %s", conf.Address)
				} else {
					log.Warning("pprof start failed, addr: %s", conf.Address)
				}
			}()
		}
	}
}

//tod
func main() {
	initConf()
	initLog()
	initBackend()
	initSignal()
	initDebug()

	server, err := initServer()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	// todo 测试用  之后删除
	var exit chan bool
	go common.StartProfile(exit)
	server.Run()
}
