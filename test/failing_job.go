package test

import (
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

var _ = lrmr.RegisterTypes(FailingStage{})

type FailingStage struct{}

func (f FailingStage) Transform(ctx lrmr.Context, in chan []lrdd.Row, emit lrmr.EmitFunc) error {
	for range in {
	}
	time.Sleep(1 * time.Second)
	panic("station")
}

func (f FailingStage) RowType() lrdd.RowType {
	return lrdd.RowTypeBytes
}

func FailingJob() *lrmr.Pipeline {
	return lrmr.Parallelize(lrdd.FromInts(1, 2, 3, 4, 5)).Do(FailingStage{})
}
