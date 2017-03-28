package hashkit

import (
	"crypto/md5"
	"errors"
	"fmt"
	"math"
	"sort"

	"ncache/backend"
)

const (
	pointsPerHash   = 4
	pointsPerServer = 160
)

func KetamaHash(key []byte, alignment int) uint32 {
	result := md5.Sum(key)
	return ((uint32(result[3+alignment*4]&0xFF) << 24) |
		(uint32(result[2+alignment*4]&0xFF) << 16) |
		(uint32(result[1+alignment*4]&0xFF) << 8) |
		(uint32(result[0+alignment*4]&0xFF) << 0))
}

// Do not consider weight
func KetamaUpdate(be backend.Backend) ([]Continuum, error) {
	nodes := be.GetNodes()
	if len(nodes) <= 0 {
		return nil, errors.New("Empty nodes")
	}

	var totalWeight int = 0
	for _, node := range nodes {
		totalWeight += node.GetWeight()
	}
	c := make([]Continuum, 0)
	for i, node := range nodes {
		pct := float32(node.GetWeight()) / float32(totalWeight)
		points := uint32((math.Floor(float64(pct*float32(pointsPerServer)/4*float32(len(nodes)) + 0.000000001))) * 4)
		for p := 1; p <= int(points/pointsPerHash); p++ {
			host := fmt.Sprintf("%s-%d", node.GetName(), p)
			for x := 0; x < pointsPerHash; x++ {
				v := KetamaHash([]byte(host), x)
				c = append(c, Continuum{index: uint32(i), value: v})
			}
		}
	}
	sort.Sort(Continuums(c))
	return c, nil
}

func KetamaDispatch(c []Continuum, hash uint32) uint32 {
	l, r, t := 0, len(c), len(c)
	for l < r {
		m := l + (r-l)>>1
		if c[m].value < hash {
			l = m + 1
		} else {
			r = m
		}
	}
	if r == t {
		r = 0
	}
	return c[r].index
}
