package test

import (
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/stage"
	"strconv"
)

type Ascending struct{}

func (a2 Ascending) IsLessThan(a, b *lrdd.Row) bool {
	var an int
	a.UnmarshalValue(&an)
	var bn int
	b.UnmarshalValue(&bn)
	return an < bn
}

var _ = stage.RegisterSorter("Ascending", &Ascending{})

type Concat struct{}

func (cc Concat) InitialValue() interface{} {
	return ""
}

func (cc Concat) Reduce(c stage.Context, prev interface{}, cur *lrdd.Row) (next interface{}, err error) {
	var n int
	cur.UnmarshalValue(&n)
	return prev.(string) + strconv.Itoa(n), nil
}

var _ = stage.RegisterReducer("Concat", &Concat{})

func Sort(m *master.Master) *lrmr.Dataset {
	data := map[string][]int{
		"foo": {9, 8, 7, 6},
		"bar": {5, 4, 3, 2},
		"baz": {9, 5, 1, 3},
	}
	return lrmr.Parallelize(data, m).
		GroupByKey().
		Sort(&Ascending{}).
		Reduce(&Concat{})
}
