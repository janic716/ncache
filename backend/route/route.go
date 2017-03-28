package route

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"ncache/backend"
	"ncache/backend/clusters"
	"ncache/backend/slice"
	"ncache/config"
	"github.com/janic716/golib/log"
)

// todo 锁机制 防止map操作时冲突
var (
	BackendMap map[string]backend.Backend
	rwLock     sync.RWMutex
)

func init() {
	BackendMap = make(map[string]backend.Backend)
}

func InitBackendMap() error {
	confs := config.GetBackendConfs()
	for name, conf := range confs {
		log.Infof("init backend: %s", name)
		t := conf.GetType()
		switch strings.ToLower(t) {
		case config.TypeCluster:
			value, ok := conf.(config.ClusterConf)
			if !ok {
				return errors.New("conf type wrong")
			}
			c, err := cluster.NewClusterWithConf(value)
			if err != nil {
				return err
			}
			rwLock.Lock()
			BackendMap[name] = c
			rwLock.Unlock()
		case config.TypeSlice:
			value, ok := conf.(config.SliceConf)
			if !ok {
				return errors.New("conf type wrong")
			}
			s, err := slice.NewSlice(value)
			if err != nil {
				return err
			}
			rwLock.Lock()
			BackendMap[name] = s
			rwLock.Unlock()
		default:
			log.Warningf("unknow  conf type. backend:%s, type:%s", t, name)
		}
	}
	return nil
}

// todo 异步的去初始化backend
func AsynInitBackend(index string) {

}

func GetBackend(index string) (be backend.Backend, err error) {
	rwLock.RLock()
	defer rwLock.RUnlock()
	be, ok := BackendMap[index]
	if !ok {
		go AsynInitBackend(index)
		return nil, fmt.Errorf("Not find backend %s", index)
	}
	return be, nil
}

func GetIndex(key string) (index string) {
	pos := strings.Index(key, ":")
	if pos == -1 {
		index = "default"
		return
	}
	index = strings.ToLower(key[:pos])
	return
}
