package lrmr

import (
	"fmt"
	"github.com/therne/lrmr/job"
)

type RunningJob struct {
	*job.Job
	master *Master

	finalStatus *job.Status
}

func (r *RunningJob) Status() job.RunningState {
	if r.finalStatus == nil {
		return job.Running
	}
	return r.finalStatus.Status
}

func (r *RunningJob) Metrics() (job.Metrics, error) {
	return r.master.jobTracker.CollectMetric(r.Job)
}

func (r *RunningJob) WaitForResult() (interface{}, error) {
	r.finalStatus = <-r.master.jobTracker.WaitForCompletion(r.Job.ID)
	if r.finalStatus.Status == job.Failed {
		return nil, fmt.Errorf("job %s failed", r.Job.ID)
	}
	return nil, nil
}
