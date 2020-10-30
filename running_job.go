package lrmr

import (
	"context"
	"os"
	"syscall"

	"github.com/pkg/errors"
	"github.com/therne/lrmr/internal/util"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/master"
)

var (
	Aborted = errors.New("job aborted")
)

type RunningJob struct {
	*job.Job
	Master *master.Master

	finalStatus *job.Status
}

func (r *RunningJob) Status() job.RunningState {
	if r.finalStatus == nil {
		return job.Running
	}
	return r.finalStatus.Status
}

func (r *RunningJob) Metrics() (job.Metrics, error) {
	return r.Master.JobTracker.CollectMetric(r.Job)
}

func (r *RunningJob) Wait() error {
	ctx, cancel := util.ContextWithSignal(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)
	defer cancel()

	return r.WaitWithContext(ctx)
}

func (r *RunningJob) WaitWithContext(ctx context.Context) error {
	select {
	case r.finalStatus = <-r.Master.JobTracker.WaitForCompletion(r.Job.ID):
		if r.finalStatus.Status == job.Failed {
			return r.finalStatus.Errors[0]
		}
	case <-ctx.Done():
		log.Info("Canceling jobs")
		_ = r.AbortWithContext(ctx)
		return ctx.Err()
	}
	return nil
}

func (r *RunningJob) Collect() ([]*lrdd.Row, error) {
	return r.Master.CollectedResults(r.Job.ID)
}

func (r *RunningJob) Abort() error {
	ctx, cancel := util.ContextWithSignal(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)
	defer cancel()

	return r.AbortWithContext(ctx)
}

func (r *RunningJob) AbortWithContext(ctx context.Context) error {
	ref := job.TaskID{
		JobID:       r.Job.ID,
		StageName:   "__input",
		PartitionID: "__master",
	}
	reporter := job.NewTaskReporter(ctx, r.Master.ClusterStates, r.Job, ref, job.NewTaskStatus())
	if err := reporter.ReportFailure(Aborted); err != nil {
		return errors.Wrap(err, "abort")
	}

	select {
	case <-r.Master.JobTracker.WaitForCompletion(r.Job.ID):
		log.Info("Cancelled!")
		break
	case <-ctx.Done():
		log.Error("Terminated")
	}
	return Aborted
}
