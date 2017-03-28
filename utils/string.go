package utils

import (
	"fmt"
	"strings"
)

func StringInSlice(needle string, slice []string) (index int) {
	for i, s := range slice {
		if needle == s {
			index = i
			return
		}
	}
	return -1
}

func HasPrefixWithInSlice(prefix string, slice []string) (index int) {
	for k, s := range slice {
		if strings.HasPrefix(s, prefix) {
			return k
		}
	}
	return -1
}

func GetStringFmt(fomat string, a ...interface{}) string {
	return fmt.Sprintf(fomat, a...)
}

func ToCamelStyle(str string, sep string) string {
	if list := strings.Split(str, sep); len(list) > 0 {
		new := make([]rune, 0, len(str))
		for _, val := range list {
			new = append(new, []rune(strings.Title(val)) ...)
		}
		return string(new)
	}
	return str
}
