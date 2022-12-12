package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/test/testutils"
)

var _ = lrmr.RegisterTypes(&MultiplyAndDouble{})

// MultiplyAndDouble doubles number of inputs each multiplied by 2.
type MultiplyAndDouble struct{}

func (m *MultiplyAndDouble) FlatMap(ctx lrmr.Context, rows []lrdd.Row) ([]lrdd.Row, error) {
	var mappedRows []lrdd.Row
	for _, row := range rows {
		n := testutils.IntValue(row)
		mappedRows = append(mappedRows, lrdd.FromInts(n*2, n*2)...)
	}

	return mappedRows, nil
}

func (m *MultiplyAndDouble) RowType() lrdd.RowType {
	return lrdd.RowTypeUint64
}

func FlatMap() *lrmr.Pipeline {
	data := make([]int, 1000)
	for i := 0; i < len(data); i++ {
		data[i] = i + 1
	}
	return lrmr.Parallelize(lrdd.FromInts(data...)).
		FlatMap(&MultiplyAndDouble{}).
		FlatMap(&MultiplyAndDouble{}).
		FlatMap(&MultiplyAndDouble{})
}
