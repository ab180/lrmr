package test

import (
	"strconv"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

var _ = lrmr.RegisterTypes(&Ascending{}, &Concat{})

type Ascending struct{}

func (a2 Ascending) IsLessThan(a, b *lrdd.Row) bool {
	an, err := strconv.Atoi(string(a.Value))
	if err != nil {
		panic(err)
	}
	bn, err := strconv.Atoi(string(b.Value))
	if err != nil {
		panic(err)
	}
	return an < bn
}

type Concat struct{}

func (cc Concat) InitialValue() lrmr.MarshalUnmarshaler {
	initVal := concatMarshalerUnmarshaler("")
	return &initVal
}

func (cc Concat) Reduce(c lrmr.Context, prev lrmr.MarshalUnmarshaler, cur *lrdd.Row,
) (next lrmr.MarshalUnmarshaler, err error) {
	n, err := strconv.Atoi(string(cur.Value))
	if err != nil {
		panic(err)
	}

	prevVal := prev.(*concatMarshalerUnmarshaler)
	reduceVal := *prevVal + concatMarshalerUnmarshaler(strconv.Itoa(n))

	return &reduceVal, nil
}

type concatMarshalerUnmarshaler string

func (c *concatMarshalerUnmarshaler) MarshalMsg([]byte) ([]byte, error) {
	return []byte(*c), nil
}

func (c *concatMarshalerUnmarshaler) UnmarshalMsg(bs []byte) ([]byte, error) {
	*c = concatMarshalerUnmarshaler(bs)
	return nil, nil
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
