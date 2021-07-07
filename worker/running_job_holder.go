package worker

import (
	"context"

	"github.com/ab180/lrmr/job"
)

type runningJobHolder struct {
	Job     *job.Job
	Tracker *job.Tracker

	ctx    context.Context
	cancel context.CancelFunc
}

func newRunningJobHolder(jobManager *job.Manager, j *job.Job) *runningJobHolder {
	ctx, cancel := context.WithCancel(context.Background())
	tracker := jobManager.Track(j)
	tracker.OnJobCompletion(func(status *job.Status) {
		cancel()
	})
	return &runningJobHolder{
		Job:     j,
		Tracker: tracker,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Context returns a Context which is cancelled after job completion.
func (rj *runningJobHolder) Context() context.Context {
	return rj.ctx
}
