package test

import (
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/stage"
	"github.com/therne/lrmr/test/testutils"
)

// Multiply multiplies input.
type Multiply struct{}

func (m *Multiply) Map(ctx stage.Context, row *lrdd.Row) (*lrdd.Row, error) {
	n := testutils.IntValue(row)
	return lrdd.Value(n * 2), nil
}

var _ = stage.RegisterMap("Multiply", &Multiply{})

func Map(m *master.Master) *lrmr.Dataset {
	data := make([]int, 1000)
	for i := 0; i < len(data); i++ {
		data[i] = i + 1
	}
	return lrmr.Parallelize(data, m).
		Map(&Multiply{}).
		Map(&Multiply{}).
		Map(&Multiply{})
}
