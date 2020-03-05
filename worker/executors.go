package worker

import (
	"context"
	"github.com/therne/lrmr/internal/logutils"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
)

type executorPool struct {
	tf     transformation.Transformation
	c      *taskContext
	shards *output.Shards

	ctx    context.Context
	Cancel context.CancelFunc

	inputChans  []chan incomingData
	Concurrency int
}

type incomingData struct {
	sender *Connection
	data   lrdd.Row
}

func launchExecutorPool(
	tf transformation.Transformation,
	c *taskContext,
	shards *output.Shards,
	concurrency, queueLen int,
) *executorPool {

	execCtx, cancel := context.WithCancel(context.Background())
	eg := &executorPool{
		tf:          tf,
		c:           c,
		shards:      shards,
		ctx:         execCtx,
		Cancel:      cancel,
		inputChans:  make([]chan incomingData, concurrency),
		Concurrency: concurrency,
	}

	// spawn executors
	for i := range eg.inputChans {
		eg.inputChans[i] = make(chan incomingData, queueLen)
		go eg.startConsume(execCtx, eg.inputChans[i], i)
	}
	return eg
}

func (ep *executorPool) startConsume(ctx context.Context, inputChan chan incomingData, executorID int) {
	var in incomingData
	defer func() {
		if err := logutils.WrapRecover(recover()); err != nil {
			if in.sender == nil {
				log.Error("Failed to initialize executor: {}", err.Pretty())
				return
			}
			in.sender.Errors <- err
		}
	}()

	// each executor owns its output writer while sharing output connections (=shards)
	out := output.NewStreamWriter(ep.shards)

	for {
		select {
		case in = <-inputChan:
			childCtx := ep.c.forkForExecutor(executorID)
			err := ep.tf.Apply(childCtx, in.data, out)
			in.sender.wg.Done()
			if err != nil {
				in.sender.Errors <- err
				ep.Cancel()
				return
			}

		case <-ctx.Done():
			// eg.Cancel() called
			return
		}
	}
}
