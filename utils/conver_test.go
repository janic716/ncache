package utils

import (
	"math"
	"strconv"
	"testing"
)

var tmap = make(map[int64][]byte)

func init() {
	var n = (maxInterBuffered-minInterBuffered+1)*2 + 100000
	for i := -n; i <= n; i++ {
		tmap[int64(i)] = []byte(strconv.Itoa(int(i)))
	}
	for i := math.MinInt64; i != 0; i = int(float64(i) / 1.1) {
		tmap[int64(i)] = []byte(strconv.Itoa(int(i)))
	}
	for i := math.MaxInt64; i != 0; i = int(float64(i) / 1.1) {
		tmap[int64(i)] = []byte(strconv.Itoa(int(i)))
	}
}

func TestInt64ToString(t *testing.T) {
	for i, b := range tmap {
		AssertMust(Int64ToString(i) == string(b))
	}
	for i := int64(minInterBuffered); i <= int64(maxInterBuffered); i++ {
		AssertMust(Int64ToString(i) == strconv.Itoa(int(i)))
	}
}

func TestBytesToInt64(t *testing.T) {
	for i, b := range tmap {
		v, err := BytesToInt64(b)
		AssertMustNoError(err)
		AssertMust(v == i)
	}
}
