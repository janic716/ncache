package hashkit

import (
	"ncache/backend"
	"math/rand"
	"sort"
)

func RandomDispatch(c []Continuum, hash uint32) uint32 {
	_ = hash
	i := rand.Intn(len(c))
	return c[i].index
}

func RandomUpdate(be backend.Backend) ([]Continuum, error) {
	nodes := be.GetNodes()
	c := make([]Continuum, 0)
	for i := range nodes {
		c = append(c, Continuum{index: uint32(i), value: 0})
	}
	sort.Sort(Continuums(c))
	return c, nil
}
