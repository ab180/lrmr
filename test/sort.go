package test

import (
	"strconv"

	"github.com/therne/lrmr"
	"github.com/therne/lrmr/lrdd"
)

var _ = lrmr.RegisterTypes(&Ascending{}, &Concat{})

type Ascending struct{}

func (a2 Ascending) IsLessThan(a, b *lrdd.Row) bool {
	var an int
	a.UnmarshalValue(&an)
	var bn int
	b.UnmarshalValue(&bn)
	return an < bn
}

type Concat struct{}

func (cc Concat) InitialValue() interface{} {
	return ""
}

func (cc Concat) Reduce(c lrmr.Context, prev interface{}, cur *lrdd.Row) (next interface{}, err error) {
	var n int
	cur.UnmarshalValue(&n)
	return prev.(string) + strconv.Itoa(n), nil
}

func Sort(sess *lrmr.Session) *lrmr.Dataset {
	data := map[string][]int{
		"foo": {9, 8, 7, 6},
		"bar": {5, 4, 3, 2},
		"baz": {9, 5, 1, 3},
	}
	return sess.Parallelize(data).
		GroupByKey().
		Sort(&Ascending{}).
		Reduce(&Concat{})
}
