package test

import (
	"context"
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
	"go.uber.org/atomic"
)

var (
	_        = lrmr.RegisterTypes(ContextCancelTestStage{})
	canceled atomic.Bool
)

type ContextCancelTestStage struct{}

func (c ContextCancelTestStage) Transform(ctx lrmr.Context, in chan []lrdd.Row, emit lrmr.EmitFunc) error {
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	timeout := ctx.Broadcast("timeout").(time.Duration)
	select {
	case <-taskCtx.Done():
		canceled.Store(true)
		return nil
	case <-time.After(timeout):
		panic("job not cancelled by context cancel")
	}
}

func (c ContextCancelTestStage) RowType() lrdd.RowType {
	return lrdd.RowTypeBytes
}

func ContextCancel(timeout time.Duration) *lrmr.Pipeline {
	return lrmr.Parallelize(lrdd.FromInts(1, 2, 3, 4, 5)).
		Broadcast("timeout", timeout).
		Do(ContextCancelTestStage{})
}

type ContextCancelWithForTestStage struct{}

func (c ContextCancelWithForTestStage) Transform(ctx lrmr.Context, in chan []lrdd.Row, emit lrmr.EmitFunc) error {
	for rows := range in {
		for range rows {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if ctx.Err() == nil {
		panic("job not cancelled by context cancel")
	}
	return nil
}

func (c ContextCancelWithForTestStage) RowType() lrdd.RowType {
	return lrdd.RowTypeBytes
}

func ContextCancelWithInputLoop() *lrmr.Pipeline {
	return lrmr.Parallelize(lrdd.FromInts(1, 2, 3, 4, 5, 6, 7, 8)).
		Do(ContextCancelWithForTestStage{})
}

type ContextCancelWithLocalPipeStage struct{}

func (c ContextCancelWithLocalPipeStage) Transform(ctx lrmr.Context, in chan []lrdd.Row, emit lrmr.EmitFunc) error {
	for rows := range in {
		for range rows {
			time.Sleep(500 * time.Millisecond)
		}
	}
	if ctx.Err() == nil {
		panic("job not cancelled by context cancel")
	}
	return nil
}

func (c ContextCancelWithLocalPipeStage) RowType() lrdd.RowType {
	return lrdd.RowTypeBytes
}

func ContextCancelWithLocalPipe() *lrmr.Pipeline {
	return lrmr.Parallelize(lrdd.FromInts(1, 2, 3, 4, 5, 6, 7, 8)).
		Do(ContextCancelWithLocalPipeStage{}).
		Do(ContextCancelWithLocalPipeStage{}).
		Do(ContextCancelWithLocalPipeStage{})
}
