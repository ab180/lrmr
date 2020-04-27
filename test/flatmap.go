package test

import (
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/stage"
	"github.com/therne/lrmr/test/testutils"
)

// MultiplyAndDouble doubles number of inputs each multiplied by 2.
type MultiplyAndDouble struct{}

func (m *MultiplyAndDouble) FlatMap(ctx stage.Context, row *lrdd.Row) ([]*lrdd.Row, error) {
	n := testutils.IntValue(row)
	return lrdd.From([]int{n * 2, n * 2}), nil
}

var _ = stage.RegisterFlatMap("MultiplyAndDouble", &MultiplyAndDouble{})

func FlatMap(m *master.Master) *lrmr.Dataset {
	data := make([]int, 1000)
	for i := 0; i < len(data); i++ {
		data[i] = i + 1
	}
	return lrmr.Parallelize(data, m).
		FlatMap(&MultiplyAndDouble{}).
		FlatMap(&MultiplyAndDouble{}).
		FlatMap(&MultiplyAndDouble{})
}
