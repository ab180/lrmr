package test

import (
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

var _ = lrmr.RegisterTypes(FailingStage{})

type FailingStage struct{}

func (f FailingStage) Transform(ctx lrmr.Context, in chan *lrdd.Row, emit func(*lrdd.Row)) error {
	for range in {
	}
	time.Sleep(1 * time.Second)
	panic("station")
}

func FailingJob() *lrmr.Pipeline {
	return lrmr.Parallelize([]int{1, 2, 3, 4, 5}).Do(FailingStage{})
}
