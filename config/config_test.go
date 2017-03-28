package config

import (
	"io"
	"os"
	"testing"

	"github.com/janic716/golib/log"
	"ncache/utils"
)

var (
	serverConfFile = GetGoPath() + "/src/ncache/config/server.json"
	dbconfFile     = GetGoPath() + "/src/ncache/config/db.json"
	dbReloadfile   = GetGoPath() + "/src/ncache/config/db_reload.json"
)

func TestParseServerConfFromFile(t *testing.T) {
	var serConf ServerConf
	var logConf log.LogConf
	var dbConfPath string

	err := parseServerConfFromFile(serverConfFile, &serConf, &logConf, &dbConfPath)
	utils.AssertMustNoError(err)
	utils.AssertMust(dbConfPath == dbconfFile)
	utils.AssertMust(serConf.Address == "127.0.0.1")
	utils.AssertMust(serConf.Name == "default")
	utils.AssertMust(serConf.ServerPort == 10000)
	utils.AssertMust(serConf.MonitorPort == 10001)
	utils.AssertMust(serConf.MaxClient == 5000)
	utils.AssertMust(serConf.MaxClientIdle == 300)
	utils.AssertMust(serConf.TimeTaskInterval == 60)
	utils.AssertMust(logConf.Module == "ncache")
	utils.AssertMust(logConf.Level == "INFO,ERROR")
}

func TestparseBackendConfFromFile(t *testing.T) {
	confs, err := parseBackendConfFromFile(dbconfFile)
	utils.AssertMustNoError(err)
	utils.AssertMust(len(confs) == 3)
}

func resetCfg() {
	Cfg = &Config{
		dbConfPath: defaultDBConfPath,
		server: &ServerConf{
			Name:             defaultName,
			Address:          defaultAddress,
			ServerPort:       defaultServerPort,
			MonitorPort:      defaultMonitorPort,
			MaxClient:        defaultMaxClient,
			MaxClientIdle:    defaultMaxClientIdle,
			TimeTaskInterval: defaultTimeTaskInterval,
		},
		log: &log.LogConf{
			Module:       defaultModule,
			Level:        defaultLevel,
			LogDir:       defaultLogDir,
			SuffixFormat: defaultSuffixFormat,
			MaxLogCount:  defaultMaxLogCount,
		},
		beConfs:       make(map[string]*NodeConf),
		beConfsReload: make(map[string]*NodeConf),
	}
}

func setOsArgs(args []string) {
	os.Args = args
}

func TestInitConfigAllFromFile(t *testing.T) {
	resetCfg()
	args := []string{
		os.Args[0],
		"-config=" + serverConfFile,
	}
	setOsArgs(args)
	err := InitConfig()
	utils.AssertMustNoError(err)
}

func TestInitConfigAllFromFlags(t *testing.T) {
	resetCfg()
	args := []string{
		os.Args[0],
		"-db-conf=" + dbconfFile,
		"-server-name=test-name",
		"-server-address=127.0.0.100",
		"-server-port=10010",
		"-monitor-port=10011",
		"-client-max-num=400",
		"-client-max-idle=30",
		"-time-task-interval=5",
		"-log-module=test-module",
		"-log-level=Error,INFO",
		"-log-dir=./",
	}
	setOsArgs(args)
	err := InitConfig()
	utils.AssertMustNoError(err)
	tempServerConf := ServerConf{
		Name:             "test-name",
		Address:          "127.0.0.100",
		ServerPort:       10010,
		MonitorPort:      10011,
		MaxClient:        400,
		MaxClientIdle:    30,
		TimeTaskInterval: 5,
	}
	utils.AssertMust(utils.DeepEqual(*Cfg.server, tempServerConf))
	tempServerConf.Name = "wrong-name"
	utils.AssertMust(!utils.DeepEqual(*Cfg.server, tempServerConf))

	utils.AssertMust(Cfg.log.Module == "test-module")
	utils.AssertMust(Cfg.log.Level == "Error,INFO")
	utils.AssertMust(Cfg.log.LogDir == "./")
}

func TestInitConfigWithFileAndFlags(t *testing.T) {
	resetCfg()
	args := []string{
		os.Args[0],
		"-config=" + serverConfFile,
		"-server-name=test-name",
		"-client-max-num=400",
	}
	setOsArgs(args)
	err := InitConfig()
	utils.AssertMustNoError(err)
	utils.AssertMust(Cfg.dbConfPath == dbconfFile)
	utils.AssertMust(Cfg.server.Name == "test-name")
	utils.AssertMust(Cfg.server.ServerPort == 10000)
	utils.AssertMust(Cfg.server.MonitorPort == 10001)
	utils.AssertMust(Cfg.server.Address == "127.0.0.1")
	utils.AssertMust(Cfg.server.MaxClient == 400)

	utils.AssertMust(len(Cfg.beConfs) == 3)
	utils.AssertMust(len(Cfg.beConfsReload) == 0)
}

func TestReloadDbConf(t *testing.T) {
	err := ReloadDbConf()
	utils.AssertMust(err == errNoNeedReload)

	path := Cfg.dbConfPath
	temp := path + ".temp"
	if utils.IsValidFile(temp) {
		utils.FileRemove(temp)
	}
	srcFile, err := os.Open(path)
	if err != nil {
		t.Fatal("Open dbconf file failed")
	}
	defer srcFile.Close()
	desFile, err := os.Create(temp)
	if err != nil {
		t.Fatal("Create New File failed")
	}
	defer desFile.Close()
	io.Copy(desFile, srcFile)
	os.Rename(temp, path)
	err = ReloadDbConf()
	utils.AssertMustNoError(err)
}

func TestReloadDbConfSpecifiedFile(t *testing.T) {
	utils.AssertMust(Cfg.dbConfPath == dbconfFile)
	utils.AssertMust(len(Cfg.beConfs) == 3)
	utils.AssertMust(len(Cfg.beConfsReload) == 3)

	err := ReloadDbConfSpecifiedFile(dbReloadfile)
	utils.AssertMustNoError(err)
	utils.AssertMust(Cfg.dbConfPath == dbReloadfile)
	utils.AssertMust(len(Cfg.beConfs) == 3)
	utils.AssertMust(len(Cfg.beConfsReload) == 2)
}
