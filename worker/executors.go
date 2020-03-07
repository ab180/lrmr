package worker

import (
	"context"
	"github.com/therne/lrmr/internal/logutils"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
)

type executor struct {
	input   chan incomingData
	c       stage.Context
	out     output.Writer
	runner  stage.Runner
	ctx     context.Context
	errChan chan error
}

type incomingData struct {
	sender *Connection
	data   lrdd.Row
}

func newExecutor(
	ctx context.Context,
	id int,
	runner stage.Runner,
	c *taskContext,
	shards *output.Shards,
	errChan chan error,
	queueLen int,
) *executor {
	// each executor owns its output writer while sharing output connections (=shards)
	out := output.NewStreamWriter(shards)

	return &executor{
		input:   make(chan incomingData, queueLen),
		c:       c.forkForExecutor(id),
		out:     out,
		runner:  runner,
		ctx:     ctx,
		errChan: errChan,
	}
}

func (e *executor) Run() {
	defer func() {
		if err := logutils.WrapRecover(recover()); err != nil {
			e.errChan <- err
		}
	}()
	for {
		select {
		case in := <-e.input:
			err := e.runner.Apply(e.c, in.data, e.out)
			in.sender.wg.Done()
			if err != nil {
				_ = in.sender.AbortTask(err)
				return
			}
		case <-e.ctx.Done():
			return
		}
	}
}

type executorPool struct {
	ctx         context.Context
	Cancel      context.CancelFunc
	Executors   []*executor
	Errors      <-chan error
	Concurrency int
}

func launchExecutorPool(
	runner stage.Runner,
	c *taskContext,
	shards *output.Shards,
	concurrency, queueLen int,
) *executorPool {
	execCtx, cancel := context.WithCancel(context.Background())
	panics := make(chan error)

	executors := make([]*executor, concurrency)
	for i := 0; i < concurrency; i++ {
		executors[i] = newExecutor(execCtx, i, runner, c, shards, panics, queueLen)
		go executors[i].Run()
	}

	return &executorPool{
		ctx:         execCtx,
		Cancel:      cancel,
		Executors:   executors,
		Errors:      panics,
		Concurrency: concurrency,
	}
}

func (ep *executorPool) Enqueue(slot int, data lrdd.Row, sender *Connection) {
	ep.Executors[slot].input <- incomingData{
		sender: sender,
		data:   data,
	}
}

func (ep *executorPool) Close() error {
	ep.Cancel()
	for _, executor := range ep.Executors {
		if err := executor.out.Flush(); err != nil {
			return err
		}
	}
	return nil
}
