package lrmr

import (
	"fmt"
	"github.com/therne/lrmr/job"
)

type RunningJob struct {
	*job.Job
	master *Master
}

func (r *RunningJob) Status() (*job.Status, error) {
	return nil, nil
}

func (r *RunningJob) WaitForResult() (interface{}, error) {
	jobStatus := <-r.master.jobTracker.WaitForCompletion(r.Job.ID)
	if jobStatus.Status == job.Failed {
		return nil, fmt.Errorf("job %s failed", r.Job.ID)
	}
	return nil, nil
}
