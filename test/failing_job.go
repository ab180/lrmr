package test

import (
	"time"

	"github.com/therne/lrmr"
	"github.com/therne/lrmr/lrdd"
)

var _ = lrmr.RegisterTypes(FailingStage{})

type FailingStage struct{}

func (f FailingStage) Transform(ctx lrmr.Context, in chan *lrdd.Row, emit func(*lrdd.Row)) error {
	for range in {
	}
	time.Sleep(1 * time.Second)
	panic("station")
	return nil
}

func FailingJob(sess *lrmr.Session) *lrmr.Dataset {
	return sess.Parallelize([]int{1, 2, 3, 4, 5}).Do(FailingStage{})
}
