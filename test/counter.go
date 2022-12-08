package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

func init() {
	lrmr.RegisterTypes(Count(), DecodeCSV())
}

type counter struct {
	value uint64
}

func Count() lrmr.Reducer {
	return &counter{}
}

func (cnt *counter) InitialValue() lrdd.MarshalUnmarshaler {
	initVal := lrdd.Uint64(0)
	return &initVal
}

func (cnt *counter) Reduce(c lrmr.Context, prev lrdd.MarshalUnmarshaler, cur *lrdd.Row,
) (next lrdd.MarshalUnmarshaler, err error) {
	c.AddMetric("Events", 1)
	prevVal := prev.(*lrdd.Uint64)
	reduceVal := *prevVal + lrdd.Uint64(1)
	cnt.value = uint64(reduceVal)
	return &reduceVal, nil
}

func (cnt *counter) RowType() lrdd.RowType {
	return lrdd.RowTypeBytes
}
