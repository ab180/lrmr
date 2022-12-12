package test

import (
	"strconv"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

var _ = lrmr.RegisterTypes(&Ascending{}, &Concat{})

type Ascending struct{}

func (a2 Ascending) IsLessThan(a, b *lrdd.Row) bool {
	an, err := strconv.Atoi(string(*a.Value.(*lrdd.Bytes)))
	if err != nil {
		panic(err)
	}
	bn, err := strconv.Atoi(string(*b.Value.(*lrdd.Bytes)))
	if err != nil {
		panic(err)
	}
	return an < bn
}

func (a2 Ascending) RowType() lrdd.RowType {
	return lrdd.RowTypeBytes
}

type Concat struct{}

func (cc Concat) InitialValue() lrdd.MarshalUnmarshaler {
	initVal := lrdd.Bytes("")
	return &initVal
}

func (cc Concat) Reduce(c lrmr.Context, prev lrdd.MarshalUnmarshaler, cur lrdd.Row,
) (next lrdd.MarshalUnmarshaler, err error) {
	n, err := strconv.Atoi(string(*cur.Value.(*lrdd.Bytes)))
	if err != nil {
		panic(err)
	}

	prevVal := prev.(*lrdd.Bytes)
	reduceVal := lrdd.NewBytes(string(*prevVal) + strconv.Itoa(n))

	return reduceVal, nil
}

func (cc Concat) RowType() lrdd.RowType {
	return lrdd.RowTypeBytes
}

func Sort() *lrmr.Pipeline {
	data := map[string][]int{
		"foo": {9, 8, 7, 6},
		"bar": {5, 4, 3, 2},
		"baz": {9, 5, 1, 3},
	}
	return lrmr.Parallelize(lrdd.FromIntSliceMap(data)).
		GroupByKey().
		Sort(&Ascending{}).
		Reduce(&Concat{})
}
