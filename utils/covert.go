package utils

import (
	"bytes"
	"strconv"
)

const (
	minInterBuffered = -128
	maxInterBuffered = 1 << 15
)

var (
	itoaOffset [maxInterBuffered - minInterBuffered + 1]uint32
	itoaBuffer string
)

func init() {
	var b bytes.Buffer
	for i := range itoaOffset {
		itoaOffset[i] = uint32(b.Len())
		b.WriteString(strconv.Itoa(i + minInterBuffered))
	}
	itoaBuffer = b.String()
}

func Int64ToString(i int64) string {
	if i >= minInterBuffered && i <= maxInterBuffered {
		beg := itoaOffset[i-minInterBuffered]
		if i == maxInterBuffered {
			return itoaBuffer[beg:]
		}
		end := itoaOffset[i-minInterBuffered+1]
		return itoaBuffer[beg:end]
	}
	return strconv.FormatInt(i, 10)
}

func BytesToInt64(b []byte) (int64, error) {
	if len(b) != 0 && len(b) < 7 {
		var neg, i = false, 0
		switch b[0] {
		case '-':
			neg = true
			fallthrough
		case '+':
			i++
		}
		if len(b) != i {
			var n int64
			for ; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
				n = int64(b[i]-'0') + n*10
			}
			if len(b) == i {
				if neg {
					n = -n
				}
				return n, nil
			}
		}
	}
	if n, err := strconv.ParseInt(string(b), 10, 64); err != nil {
		return 0, err
	} else {
		return n, nil
	}
}
