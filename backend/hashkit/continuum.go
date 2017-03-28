package hashkit

type Continuum struct {
	index uint32
	value uint32
}

type Continuums []Continuum

func (a Continuums) Len() int           { return len(a) }
func (a Continuums) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Continuums) Less(i, j int) bool { return a[i].value < a[j].value }
