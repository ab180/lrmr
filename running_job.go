package lrmr

import (
	"fmt"
	"github.com/therne/lrmr/node"
)

type RunningJob struct {
	*node.Job
	master *Master
}

func (r *RunningJob) Status() (*node.JobStatus, error) {
	return nil, nil
}

func (r *RunningJob) WaitForResult() (interface{}, error) {
	jobStatus := <-r.master.jobTracker.WaitForCompletion(r.Job.ID)
	if jobStatus.Status == node.Failed {
		return nil, fmt.Errorf("job %s failed", r.Job.ID)
	}
	return nil, nil
}
