package playground

import (
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/lrdd"
)

func init() {
	lrmr.RegisterTypes(Count(), DecodeJSON())
}

type counter struct {
	value uint64
}

func Count() lrmr.Reducer {
	return &counter{}
}

func (cnt *counter) InitialValue() interface{} {
	return uint64(0)
}

func (cnt *counter) Reduce(c lrmr.Context, prev interface{}, cur *lrdd.Row) (next interface{}, err error) {
	c.AddMetric("Events", 1)
	cnt.value = prev.(uint64) + 1
	return cnt.value, nil
}
