package common

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/shirou/gopsutil/load"
	"github.com/shirou/gopsutil/process"
	"github.com/shirou/gopsutil/cpu"
	"ncache/utils"
)

const (
	_ = 1 << (iota * 10)
	kSize
	mSize
	gSize
)

var f *os.File

func init() {
	var (
		err error
	)
	currentPath := GetCurrentPath()
	tempFileName := "profile" + "_" + strconv.FormatInt(time.Now().Unix(), 10)
	name := currentPath + "/" + tempFileName
	if !utils.IsValidDir(name) {
		utils.FileRemove(name)

	}
	f, err = os.Create(name)
	if err != nil {
		fmt.Printf("Creat File Failed, error info: %s\n", err.Error())
		os.Exit(1)
	}
}

func GetReadableSize(size uint64) string { return getReadableSize(size) }

func getReadableSize(size uint64) string {
	str := ""
	if size/gSize > 0 {
		str = str + strconv.Itoa(int(size/gSize)) + "g"
		size %= gSize
	}

	if size/mSize > 0 {
		str = str + strconv.Itoa(int(size/mSize)) + "m"
		size %= mSize
	}
	if size/kSize > 0 {
		str = str + strconv.Itoa(int(size/kSize)) + "k"
		size %= kSize
	}
	if size > 0 {
		str = str + strconv.Itoa(int(size)) + "b"
	}
	if str == "" {
		str = "0"
	}
	return str
}

func StartProfile(stop chan bool) error {
	pid := os.Getpid()

	proObj, _ := process.NewProcess(int32(pid))
	for {
		select {
		case <-stop:
			break
		default:
			time.Sleep(1 * time.Second)
		}
		memInfo, _ := proObj.MemoryInfo()
		memPer, _ := proObj.MemoryPercent()
		cpuPer, _ := proObj.Percent(0)
		threadsNum, _ := proObj.NumThreads()
		loadInfo, _ := load.Avg()
		numFds, _ := proObj.NumFDs()
		str := utils.GetStringFmt(
			"RSS: %s, VMS: %s, SWAP: %s, MemPer: %f, CPU: %f, threads num: %04d, load:[1] %f, [5]%f, [15]%f, fd num:%d",
			getReadableSize(memInfo.RSS),
			getReadableSize(memInfo.VMS),
			getReadableSize(memInfo.Swap),
			memPer,
			cpuPer,
			threadsNum,
			loadInfo.Load1,
			loadInfo.Load5,
			loadInfo.Load15,
			numFds,
		)
		cpuInfo, _ := cpu.Times(false)
		f.WriteString(time.Now().String() + "\t")
		f.WriteString(str)
		f.WriteString("\n")
		if len(cpuInfo) == 1 {
			f.WriteString(cpuInfo[0].String() + "\n")
		}
	}
}
