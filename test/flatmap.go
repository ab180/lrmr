package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/test/testutils"
)

var _ = lrmr.RegisterTypes(&MultiplyAndDouble{})

// MultiplyAndDouble doubles number of inputs each multiplied by 2.
type MultiplyAndDouble struct{}

func (m *MultiplyAndDouble) FlatMap(ctx lrmr.Context, row *lrdd.Row) ([]*lrdd.Row, error) {
	n := testutils.IntValue(row)
	return lrdd.From([]int{n * 2, n * 2}), nil
}

func FlatMap() *lrmr.Pipeline {
	data := make([]int, 1000)
	for i := 0; i < len(data); i++ {
		data[i] = i + 1
	}
	return lrmr.Parallelize(data).
		FlatMap(&MultiplyAndDouble{}).
		FlatMap(&MultiplyAndDouble{}).
		FlatMap(&MultiplyAndDouble{})
}
