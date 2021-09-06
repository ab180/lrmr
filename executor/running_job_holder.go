package executor

import (
	"context"

	"github.com/ab180/lrmr/internal/serialization"
	"github.com/ab180/lrmr/job"
	"go.uber.org/atomic"
)

type runningJobHolder struct {
	Job        *job.Job
	Tasks      []*TaskExecutor
	Broadcasts serialization.Broadcast
	Reporter   StatusReporter
	IsStarted  atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
}

func newRunningJobHolder(j *job.Job, broadcasts serialization.Broadcast) *runningJobHolder {
	ctx, cancel := context.WithCancel(context.Background())
	return &runningJobHolder{
		Job:        j,
		Broadcasts: broadcasts,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (rj *runningJobHolder) Start(reporter StatusReporter) {
	rj.IsStarted.Store(true)
	rj.Reporter = reporter

	// make Context inherit job context from reporter
	go rj.cancelJobContextByReporter()
}

func (rj *runningJobHolder) cancelJobContextByReporter() {
	<-rj.Reporter.JobContext().Done()
	rj.cancel()
}

// Context returns a Context which is cancelled after job completion.
func (rj *runningJobHolder) Context() context.Context {
	return rj.ctx
}
