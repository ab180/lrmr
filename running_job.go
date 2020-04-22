package lrmr

import (
	"github.com/pkg/errors"
	"github.com/therne/lrmr/job"
	"os"
	"os/signal"
	"syscall"
)

var (
	Aborted = errors.New("job aborted")
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

func (r *RunningJob) Wait() error {
	sigTerm := make(chan os.Signal)
	signal.Notify(sigTerm, os.Interrupt, os.Kill, syscall.SIGTERM)

	select {
	case r.finalStatus = <-r.master.jobTracker.WaitForCompletion(r.Job.ID):
		if r.finalStatus.Status == job.Failed {
			return errors.Errorf("job %s failed", r.Job.ID)
		}
	case <-sigTerm:
		log.Info("Canceling jobs")
		return r.Abort()
	}
	return nil
}

func (r *RunningJob) Abort() error {
	ref := job.TaskReference{
		JobID:     r.Job.ID,
		StageName: "__input",
		TaskID:    "__master",
	}
	if err := r.master.jobReporter.ReportFailure(ref, Aborted); err != nil {
		return errors.Wrap(err, "abort")
	}

	sigTerm := make(chan os.Signal)
	signal.Notify(sigTerm, os.Interrupt, os.Kill, syscall.SIGTERM)

	select {
	case <-r.master.jobTracker.WaitForCompletion(r.Job.ID):
		log.Info("Cancelled!")
		break
	case <-sigTerm:
		log.Error("Terminated")
	}
	return Aborted
}
