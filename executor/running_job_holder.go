package executor

import (
	"context"

	"github.com/ab180/lrmr/job"
	lrmrmetric "github.com/ab180/lrmr/metric"
)

type runningJobHolder struct {
	Job     *job.Job
	Tracker StatusReporter

	ctx    context.Context
	cancel context.CancelFunc

	metric lrmrmetric.Repository
}

func newRunningJobHolder(j *job.Job, tracker StatusReporter) *runningJobHolder {
	return &runningJobHolder{
		Job:     j,
		Tracker: tracker,
	}
}

// Context returns a Context which is cancelled after job completion.
func (rj *runningJobHolder) Context() context.Context {
	return rj.Tracker.JobContext()
}
