package test

import (
	"strconv"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/test/testutils"
)

var _ = lrmr.RegisterTypes(&Multiply{})

// Multiply multiplies input.
type Multiply struct{}

func (m *Multiply) Map(ctx lrmr.Context, row *lrdd.Row) (*lrdd.Row, error) {
	n := testutils.IntValue(row)
	return &lrdd.Row{Value: []byte(strconv.Itoa(n * 2))}, nil
}

func Map() *lrmr.Pipeline {
	data := make([]int, 1000)
	for i := 0; i < len(data); i++ {
		data[i] = i + 1
	}
	return lrmr.Parallelize(lrdd.FromInts(data...)).
		Map(&Multiply{}).
		Map(&Multiply{}).
		Map(&Multiply{})
}
