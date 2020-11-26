package test

import (
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

var _ = lrmr.RegisterTypes(HaltForMasterFailure{})

type HaltForMasterFailure struct{}

func (f HaltForMasterFailure) Transform(ctx lrmr.Context, in chan *lrdd.Row, emit func(*lrdd.Row)) error {
	time.Sleep(1 * time.Second)
	for range in {
		ctx.AddMetric("Input", 1)
	}
	return nil
}

func RunWithoutMaster(sess *lrmr.Session) *lrmr.Dataset {
	return sess.Parallelize([]int{1, 2, 3, 4, 5}).
		Do(HaltForMasterFailure{})
}
