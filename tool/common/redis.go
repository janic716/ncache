package common

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"ncache/utils"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	testBaseDir = "/Users/liuxinjun/temp/"
)

type Record struct {
	master string
	slaves []string
}

var record map[string]*Record

func init() {
	record = make(map[string]*Record, 0)
}

type clusterConfig struct {
	Master string `json:"master"`
	Slaves string `json:"slaves"`
}

func CleanBaseDir() {
	cmd := exec.Command("rm", "-rf", testBaseDir)
	err := cmd.Run()
	if err != nil {
		os.Exit(1)
	}
}

func GetCurrentPath() string {
	file, _ := exec.LookPath(os.Args[0])
	path, _ := filepath.Abs(file)
	path = filepath.Dir(path)
	return path
}

func StartRedis(path string) error {
	var (
		err error
	)
	workPath := GetCurrentPath()
	defer func() {
		if err != nil {
			fmt.Println("start redis error", err.Error())
			ShutDownRedis()
		}
		os.Chdir(workPath)
	}()
	CleanBaseDir()
	contents, err := utils.FileReadContents(path)
	if err != nil {
		return err
	}
	b := []byte(contents)
	jmap, err := utils.JsonDecode2Rawmap(b)
	if err != nil {
		return err
	}
	for k, v := range jmap {
		var cluster clusterConfig
		fmt.Println(k, string(*v))
		err := utils.JsonDecode2Stru(*v, &cluster)
		if err != nil {
			return err
		}
		fmt.Println("cluster:", cluster)
		err = startCluster(k, cluster)
		if err != nil {
			return err
		}
	}
	return nil
}

func setReplication(slave, master string) error {
	conn, err := redis.Dial("tcp", slave)
	if err != nil {
		return err
	}
	ip_port := strings.Split(master, ":")
	fmt.Println("SLAVEOF", ip_port[0], ip_port[1])
	_, err = conn.Do("SLAVEOF", ip_port[0], ip_port[1])
	if err != nil {
		return err
	}
	return nil
}

func startCluster(name string, v clusterConfig) error {
	var (
		err     error
		cmd     *exec.Cmd
		address []string
	)
	clusterBasePath := strings.Join([]string{testBaseDir, name}, "")
	fmt.Println(clusterBasePath)
	masterPath := clusterBasePath + "/" + "master"
	fmt.Println(masterPath)
	if err = os.MkdirAll(masterPath, 0777); err != nil {
		return err
	}
	if err = os.Chdir(masterPath); err != nil {
		return err
	}
	address = strings.Split(v.Master, ":")
	masterIp := address[0]
	masterPort := address[1]
	fmt.Println("redis-server", "--port", masterPort)
	cmd = exec.Command("redis-server", "--port", masterPort)
	if err = cmd.Start(); err != nil {
		return err
	}
	time.Sleep(time.Millisecond * 50)
	if _, ok := record[name]; !ok {
		record[name] = &Record{}
	}
	record[name].master = masterIp + ":" + masterPort
	slaves := strings.Split(v.Slaves, ",")
	for index, slave := range slaves {
		address = strings.Split(slave, ":")
		slaveIp := address[0]
		slavePort := address[1]
		slavePath := strings.Join([]string{clusterBasePath, "/", "slave", strconv.Itoa(index + 1)}, "")
		fmt.Println(slavePath)
		if err = os.MkdirAll(slavePath, 0777); err != nil {
			return err
		}
		if err = os.Chdir(slavePath); err != nil {
			return err
		}
		fmt.Println("redis-server", "--port", slavePort)
		cmd = exec.Command("redis-server", "--port", slavePort)
		if err = cmd.Start(); err != nil {
			return err
		}
		time.Sleep(time.Millisecond * 50)
		err = setReplication(slave, v.Master)
		if err != nil {
			fmt.Printf("Set Replication failed. cluster:%s, master: %s, slave: %s\n", name, v.Master, slave)
		}
		time.Sleep(time.Millisecond * 10)
		record[name].slaves = append(record[name].slaves, slaveIp+":"+slavePort)
	}
	return nil
}

func ShutDownRedis() {
	fmt.Println(*record["default"])
	for k, v := range record {
		log.Println("Shutdown cluster", k)
		// flush all
		log.Printf("Shoudonw cluster %s, master %s", k, v.master)
		connectAndShutdonw(v.master)
		for _, slave := range v.slaves {
			log.Printf("Shoudonw cluster %s, slave %s\n", k, slave)
			connectAndShutdonw(slave)
		}
	}
}

func connectAndShutdonw(address string) {
	conn, err := redis.Dial("tcp", address)
	if err != nil {
		fmt.Printf("Err when connect %s", address)
		return
	}

	_, err = conn.Do("FLUSHALL")
	time.Sleep(1)
	_, err = conn.Do("SHUTDOWN")
	time.Sleep(1)
	if err != nil {
		if err != io.EOF {
			fmt.Printf("Err When shutdone %s\n", err.Error())
		}
	}
}
