package test

import (
	"strconv"

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

func (cnt *counter) InitialValue() lrmr.MarshalerUnmarshaler {
	initVal := counterMarshalerUnmarshaler(0)
	return &initVal
}

func (cnt *counter) Reduce(c lrmr.Context, prev lrmr.MarshalerUnmarshaler, cur *lrdd.Row) (next lrmr.MarshalerUnmarshaler, err error) {
	c.AddMetric("Events", 1)
	prevVal := prev.(*counterMarshalerUnmarshaler)
	reduceVal := *prevVal + counterMarshalerUnmarshaler(1)
	cnt.value = uint64(reduceVal)
	return &reduceVal, nil
}

type counterMarshalerUnmarshaler uint64

func (c *counterMarshalerUnmarshaler) MarshalMsg([]byte) ([]byte, error) {
	return []byte(strconv.FormatUint(uint64(*c), 10)), nil
}

func (c *counterMarshalerUnmarshaler) UnmarshalMsg(bs []byte) ([]byte, error) {
	v, err := strconv.ParseUint(string(bs), 10, 64)
	if err != nil {
		return nil, err
	}

	*c = counterMarshalerUnmarshaler(v)

	return nil, nil
}
