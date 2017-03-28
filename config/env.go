package config

import (
	"os"
)

func GetGoPath() (path string) {
	path = os.Getenv("GOPATH")
	return
}

func GetPid() (pid int) {
	pid = os.Getpid()
	return
}
