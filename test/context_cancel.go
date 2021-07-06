package test

import (
	"context"
	"log"
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

var _ = lrmr.RegisterTypes(ContextCancelTestStage{})

type ContextCancelTestStage struct{}

func (c ContextCancelTestStage) Transform(ctx lrmr.Context, in chan *lrdd.Row, emit func(*lrdd.Row)) error {
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	timeout := ctx.Broadcast("timeout").(time.Duration)
	log.Println("timeout is ", timeout)
	select {
	case <-taskCtx.Done():
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
