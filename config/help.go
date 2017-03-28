package config

import (
	"fmt"
	"os"
)

var (
	usageLine = `usage: ncache [flags]
       start an ncache server

       ncache --version
       show the version of ncache

       ncache -h | --help
       show the help information about ncache
	`

	flagsLine = `
	//todo fill flags message
	`
	version = "ncache version: 0.0.1"
)

func printUsage() {
	fmt.Println(usageLine)
}

func printFlags() {
	fmt.Println(flagsLine)
	os.Exit(0)
}

func printVersion() {
	fmt.Println(version)
	os.Exit(0)
}
