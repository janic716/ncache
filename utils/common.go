package utils

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

//为空的情况: 空字符串, false, 引用空, array/map/slice 长度为0, 数值为0
func Empty(v interface{}) bool {
	if v == nil {
		return true
	}
	switch a := v.(type) {
	case string:
		return a == ""
	case bool:
		return !a
	}
	val := reflect.ValueOf(v)
	switch val.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return val.Len() == 0
	case reflect.Bool:
		return !val.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return val.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return val.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return val.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return val.IsNil()
	}
	return false
}

//只要有一个为空, 返回true
func AnyEmpty(values ...interface{}) bool {
	for v := range values {
		if Empty(v) {
			return true
		}
	}
	return false
}

//所有为空, 返回true
func AllEmpty(values ...interface{}) bool {
	for v := range values {
		if !Empty(v) {
			return false
		}
	}
	return true

}

func IsNumeric(v string) bool {
	if _, err := strconv.ParseInt(v, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(v, 64); err == nil {
		return true
	}
	if strings.HasPrefix(v, "0x") || strings.HasPrefix(v, "0X") {
		if _, err := strconv.ParseInt(v, 0, 64); err == nil {
			return true
		}
	}
	return false
}

func Println(debug bool, a ...interface{}) (int, error) {
	if debug {
		return fmt.Println(a)
	}
	return 0, nil
}

func Printf(debug bool, format string, a ...interface{}) (n int, err error) {
	if debug {
		return fmt.Printf(format, a)
	}
	return 0, err
}
