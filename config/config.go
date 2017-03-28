package config

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/janic716/golib/log"
	"ncache/utils"
	"strings"
)

// 全局变量
var Cfg *Config

var (
	errNoNeedReload = errors.New("Not need to Reload Db conf")
)

type Config struct {
	dbConfPath    string
	fileInfo      os.FileInfo
	server        *ServerConf
	log           *log.LogConf
	beConfs       map[string]Conf
	beConfsReload map[string]Conf
}

func init() {
	// 初始化并设置默认值
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
		beConfs:       make(map[string]Conf),
		beConfsReload: make(map[string]Conf),
	}
}

func printDebug(exit bool) {
	fmt.Println("\n\n")
	fmt.Println("dbconfpath")
	fmt.Println(Cfg.dbConfPath)
	fmt.Println("server")
	fmt.Println(*Cfg.server)
	fmt.Println("log")
	fmt.Println(*Cfg.log)
	fmt.Println("nodes")
	for k, v := range Cfg.beConfs {
		fmt.Println(k, v)
	}
	fmt.Println("nodesReload")
	for k, v := range Cfg.beConfsReload {
		fmt.Println(k, v)
	}
	fmt.Println("\n\n")
	if exit {
		os.Exit(1)
	}
}

func InitConfig() error {
	var err error
	var conf string
	// 首先判断参数中是否有server配置文件选线并取出
	cmdStrings := os.Args[1:]
	if index := utils.StringInSlice("-config", cmdStrings); index != -1 {
		if len(cmdStrings) < index+2 {
			printFlags()
		}
		conf = cmdStrings[index+1]
	} else if index := utils.HasPrefixWithInSlice("-config=", cmdStrings); index != -1 {
		conf = cmdStrings[index][len("-config="):]
	}
	// 解析配置文件并替换默认值
	if conf != "" {
		var sFile ServerConf
		var lFile log.LogConf
		var dbPathFile string
		if err = parseServerConfFromFile(conf, &sFile, &lFile, &dbPathFile); err != nil {
			return err
		}
		if dbPathFile != "" {
			Cfg.dbConfPath = dbPathFile
		}
		if err = mergeServerConf(Cfg.server, &sFile); err != nil {
			return err
		}
		if err = mergeLogConf(Cfg.log, &lFile); err != nil {
			return err
		}
	}
	// 解析命令行参数
	fs := flag.NewFlagSet("ncache", flag.ContinueOnError)
	fs.Usage = printUsage
	// configuration file
	fs.StringVar(&conf, "config", "", "configuration file to start server")
	var dbConfFlag string
	fs.StringVar(&dbConfFlag, "db-conf", "", "configuration file to describe db nodes")
	// server
	var sFlag ServerConf
	fs.StringVar(&sFlag.Name, "server-name", defaultName, "name of server")
	fs.StringVar(&sFlag.Address, "server-address", defaultAddress, "")
	fs.IntVar(&sFlag.ServerPort, "server-port", defaultServerPort, "port of server")
	fs.IntVar(&sFlag.MonitorPort, "monitor-port", defaultMonitorPort, "port of web monitor")
	fs.IntVar(&sFlag.MaxClient, "client-max-num", defaultMaxClient, "max client num")
	fs.IntVar(&sFlag.MaxClientIdle, "client-max-idle", defaultMaxClientIdle, "client max idle time in seconds")
	fs.IntVar(&sFlag.TimeTaskInterval, "time-task-interval", defaultTimeTaskInterval,
		"time task interval")
	fs.BoolVar(&sFlag.PprofEnable, "pprof-enable", true, "pprof enable, default true")
	fs.StringVar(&sFlag.PprofAddr, "pprof-addr", "localhost:6060", "pprof http addr")
	// log
	var lFlag log.LogConf
	fs.StringVar(&lFlag.Module, "log-module", defaultModule, "log module name")
	fs.StringVar(&lFlag.Level, "log-level", defaultLevel, "log level")
	fs.StringVar(&lFlag.LogDir, "log-dir", defaultLogDir, "log storage directory")
	fs.StringVar(&lFlag.SuffixFormat, "log-suffix", defaultSuffixFormat, "log suffix format")
	fs.IntVar(&lFlag.MaxLogCount, "log-max-cout", defaultMaxLogCount, "log max count")

	// version
	var printVer bool
	fs.BoolVar(&printVer, "version", false, "Print version and exit")

	flagsStrings := os.Args[1:]
	perr := fs.Parse(flagsStrings)
	switch perr {
	case nil:
	case flag.ErrHelp:
		printFlags()
	default:
		os.Exit(2)
	}
	// 打印version
	if printVer {
		printVersion()
	}

	if err = Cfg.mergeWithFlags(sFlag, lFlag, dbConfFlag, flagsStrings); err != nil {
		return err
	}
	path := Cfg.dbConfPath
	if !utils.IsValidFile(path) {
		return errors.New("Invalid DB Configuration File")
	}

	nodesConf, err := parseBackendConfFromFile(path)
	if err != nil {
		return err
	}

	Cfg.beConfs = nodesConf
	Cfg.fileInfo, _ = os.Stat(path)
	return nil
}

func ReloadDbConf() error { return Cfg.reloadDbConf() }

func (cfg *Config) reloadDbConf() error {
	if cfg == nil {
		return errors.New("Conf is nil")
	}
	dbConfPath := cfg.dbConfPath
	fileInfo, err := os.Stat(dbConfPath)
	if err != nil {
		return err
	}
	if fileInfo.ModTime() == cfg.fileInfo.ModTime() {
		return errNoNeedReload
	}
	if dbConfPath == "" {
		return errors.New("Empty DB Configuration File Path")
	}
	nodesConf, err := parseBackendConfFromFile(dbConfPath)
	if err != nil {
		return err
	}
	cfg.beConfsReload = nodesConf
	cfg.fileInfo, _ = os.Stat(dbConfPath)
	return nil
}

func ReloadDbConfSpecifiedFile(file string) error { return Cfg.reloadDbConfSpecifiedFile(file) }

func (cfg *Config) reloadDbConfSpecifiedFile(file string) error {
	if !utils.IsValidFile(file) {
		return errors.New("Specified file is invalid")
	}
	if utils.TwoFileSameAbs(file, cfg.dbConfPath) {
		return cfg.reloadDbConf()
	}

	nodesConf, err := parseBackendConfFromFile(file)
	if err != nil {
		return err
	}
	//
	cfg.beConfsReload = nodesConf
	cfg.dbConfPath = file
	cfg.fileInfo, _ = os.Stat(file)
	return nil
}

func GetServerConf() (*ServerConf, error) { return Cfg.getServerConf() }
func (cfg *Config) getServerConf() (*ServerConf, error) {
	if cfg.server == nil {
		return nil, errors.New("Server conf is nil")
	}
	return cfg.server, nil
}

func GetLogConf() (*log.LogConf, error) { return Cfg.getLogConf() }
func (cfg *Config) getLogConf() (*log.LogConf, error) {
	if cfg.log == nil {
		return nil, errors.New("Log conf is nil")
	}
	return cfg.log, nil
}

func GetBackendConfs() map[string]Conf {
	return Cfg.beConfs
}

func parseServerConfFromFile(path string, sconf *ServerConf, logconf *log.LogConf, dbconfPath *string) error {
	contents, err := utils.FileReadContents(path)
	if err != nil {
		return err
	}
	b := []byte(contents)
	rawMap, err := utils.JsonDecode2Rawmap(b)
	if err != nil {
		return err
	}

	for key, value := range rawMap {
		switch key {
		case "dbconf":
			if *dbconfPath, err = strconv.Unquote(string(*value)); err != nil {
				return err
			}
		case "server":
			if err = utils.JsonDecode2Stru(*value, sconf); err != nil {
				return err
			}
		case "log":
			if err = utils.JsonDecode2Stru(*value, logconf); err != nil {
				return err
			}
		default:
		}
	}
	return nil
}

func ParseBackendConfFromFile(file string) (map[string]Conf, error) {
	return parseBackendConfFromFile(file)
}
func parseBackendConfFromFile(file string) (map[string]Conf, error) {
	contents, err := utils.FileReadContents(file)
	if err != nil {
		return nil, err
	}
	b := []byte(contents)
	rawMap, err := utils.JsonDecode2Rawmap(b)
	if err != nil {
		return nil, err
	}

	var beConfs = make(map[string]Conf)

	for k, v := range rawMap {
		temp, err := utils.JsonDecode2Map(*v)
		if err != nil {
			return nil, err
		}
		str, ok := temp["type"]
		if !ok {
			return nil, errors.New("Config missing type field")
		}
		typestr, ok := str.(string)
		if !ok {
			return nil, errors.New("type format error")
		}
		switch strings.ToLower(typestr) {
		case TypeCluster:
			var temp ClusterConf
			if err = utils.JsonDecode2Stru([]byte(*v), &temp); err != nil {
				return nil, err
			}
			beConfs[k] = temp
		case TypeSlice:
			var temp SliceConf
			if err = utils.JsonDecode2Stru([]byte(*v), &temp); err != nil {
				return nil, err
			}
			beConfs[k] = temp
		default:
			return nil, errors.New("Unknow config type")

		}
	}
	return beConfs, nil
}

func (cfg *Config) mergeWithFlags(sConf ServerConf, lConf log.LogConf, path string, flags []string) (err error) {
	if cfg == nil || cfg.server == nil || cfg.log == nil {
		err = errors.New("Config not initialize")
		return
	}

	for _, v := range flags {
		if v[0] != '-' {
			continue
		}
		vFlag := v[1:]
		if index := strings.Index(vFlag, "="); index != -1 {
			vFlag = vFlag[0:index]
		}



		switch vFlag {
		case "db-conf":
			cfg.dbConfPath = path
		case "server-name":
			cfg.server.Name = sConf.Name
		case "server-address":
			cfg.server.Address = sConf.Address
		case "server-port":
			cfg.server.ServerPort = sConf.ServerPort
		case "monitor-port":
			cfg.server.MonitorPort = sConf.MonitorPort
		case "client-max-num":
			cfg.server.MaxClient = sConf.MaxClient
		case "client-max-idle":
			cfg.server.MaxClientIdle = sConf.MaxClientIdle
		case "time-task-interval":
			cfg.server.TimeTaskInterval = sConf.TimeTaskInterval
		case "log-module":
			cfg.log.Module = lConf.Module
		case "log-level":
			cfg.log.Level = lConf.Level
		case "log-dir":
			cfg.log.LogDir = lConf.LogDir
		case "log-suffix":
			cfg.log.SuffixFormat = lConf.SuffixFormat
		case "log-max-cout":
			cfg.log.MaxLogCount = lConf.MaxLogCount
		case "pprof-enable":
			cfg.server.PprofEnable = sConf.PprofEnable
		case "pprof-addr":
			cfg.server.PprofAddr = sConf.PprofAddr
		default:
			continue
		}
	}
	return nil
}
