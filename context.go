package lrmr

import (
	"context"
	"time"

	"github.com/ab180/lrmr/transformation"
)

type Context interface {
	transformation.Context
}

type cancelableContext struct {
	transformation.Context
	cancelCtx context.Context
}

func contextWithCancel(ctx Context) (Context, context.CancelFunc) {
	cancelCtx, cancel := context.WithCancel(ctx)
	return &cancelableContext{
		Context:   ctx,
		cancelCtx: cancelCtx,
	}, cancel
}

func (c cancelableContext) Err() error {
	return c.cancelCtx.Err()
}

func (c cancelableContext) Done() <-chan struct{} {
	return c.cancelCtx.Done()
}

func (c cancelableContext) Deadline() (deadline time.Time, ok bool) {
	return c.cancelCtx.Deadline()
}
