package test

import (
	"context"
	"log"
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

func (c ContextCancelTestStage) Transform(ctx lrmr.Context, in chan *lrdd.Row, emit func(*lrdd.Row)) error {
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	timeout := ctx.Broadcast("timeout").(time.Duration)
	log.Println("timeout is ", timeout)
	select {
	case <-taskCtx.Done():
		canceled.Store(true)
		log.Println("deadline")
		return nil
	case <-time.After(timeout):
		panic("job not cancelled by context cancel")
	}
}

func ContextCancel(sess *lrmr.Session, timeout time.Duration) *lrmr.Dataset {
	return sess.Parallelize([]int{1, 2, 3, 4, 5}).
		Broadcast("timeout", timeout).
		Do(ContextCancelTestStage{})
}

type ContextCancelWithForTestStage struct{}

func (c ContextCancelWithForTestStage) Transform(ctx lrmr.Context, in chan *lrdd.Row, emit func(*lrdd.Row)) error {
	for range in {
		time.Sleep(200 * time.Millisecond)
	}
	if ctx.Err() == nil {
		panic("job not cancelled by context cancel")
	}
	log.Println("deadline")
	return nil
}

func ContextCancelWithInputLoop(sess *lrmr.Session, timeout time.Duration) *lrmr.Dataset {
	return sess.Parallelize([]int{1, 2, 3, 4, 5, 6, 7, 8}).
		Do(ContextCancelWithForTestStage{})
}
