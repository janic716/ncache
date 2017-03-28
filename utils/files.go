package utils

import (
	"io/ioutil"
	"os"
	"path"
	Path "path/filepath"
)

func IsValidDir(dir string) bool {
	fileInfo, err := os.Stat(dir)
	if err != nil {
		return false
	}
	return fileInfo.IsDir()
}

func IsValidFile(filename string) bool {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		return false
	}
	return fileInfo.Mode().IsRegular()
}

func IsValidDirOrFile(name string) bool {
	fileInfo, err := os.Stat(name)
	if err != nil {
		return false
	}
	return fileInfo.IsDir() || fileInfo.Mode().IsRegular()
}

func FilePutContents(filename, content string) (int, error) {
	return WriteBytesToFile(filename, []byte(content))
}

func FileReadContents(filename string) (contents string, err error) {
	data, err := ioutil.ReadFile(filename)
	if err == nil {
		contents = string(data)
	}
	return
}

func FileRemove(filename string) (err error) {
	if IsValidFile(filename) {
		err = os.Remove(filename)
	}
	return
}

func WriteBytesToFile(filename string, bytes []byte) (int, error) {
	os.MkdirAll(path.Dir(filename), os.ModePerm)
	fw, err := os.Create(filename)
	if err != nil {
		return 0, err
	}
	defer fw.Close()
	return fw.Write(bytes)
}

func TwoFileSameAbs(filename1, filename2 string) bool {
	var (
		abs1, abs2 string
		err        error
	)
	if abs1, err = Path.Abs(filename1); err != nil {
		return false
	}
	if abs2, err = Path.Abs(filename2); err != nil {
		return false
	}
	return abs1 == abs2
}
