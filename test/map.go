package test

import (
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/test/testutils"
)

var _ = lrmr.RegisterTypes(&Multiply{})

// Multiply multiplies input.
type Multiply struct{}

func (m *Multiply) Map(ctx lrmr.Context, row *lrdd.Row) (*lrdd.Row, error) {
	n := testutils.IntValue(row)
	return lrdd.Value(n * 2), nil
}

func Map(sess *lrmr.Session) *lrmr.Dataset {
	data := make([]int, 1000)
	for i := 0; i < len(data); i++ {
		data[i] = i + 1
	}
	return sess.Parallelize(data).
		Map(&Multiply{}).
		Map(&Multiply{}).
		Map(&Multiply{})
}
