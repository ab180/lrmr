package lrmr

import (
	"context"
	"os"
	"sync"
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
	statusMu    sync.RWMutex
}

func (r *RunningJob) Status() job.RunningState {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()

	if r.finalStatus == nil {
		return job.Running
	}
	return r.finalStatus.Status
}

func (r *RunningJob) Metrics() (job.Metrics, error) {
	statuses, err := r.Master.JobManager.ListTaskStatusesInJob(context.TODO(), r.Job.ID)
	if err != nil {
		return nil, errors.Wrap(err, "list task status")
	}

	metric := make(job.Metrics)
	for _, status := range statuses {
		metric = metric.Sum(status.Metrics)
	}
	return metric, nil
}

func (r *RunningJob) Wait() error {
	ctx, cancel := util.ContextWithSignal(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)
	defer cancel()

	return r.WaitWithContext(ctx)
}

func (r *RunningJob) WaitWithContext(ctx context.Context) error {
	defer r.dumpMetricsOnBackground()

	jobWaitChan := make(chan struct{}, 1)
	r.Master.JobTracker.OnJobCompletion(r.Job, func(j *job.Job, status *job.Status) {
		r.statusMu.Lock()
		r.finalStatus = status
		r.statusMu.Unlock()

		jobWaitChan <- struct{}{}
	})

	select {
	case <-jobWaitChan:
		if r.Status() == job.Failed {
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
	defer r.dumpMetricsOnBackground()
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
	reporter := job.NewTaskReporter(ctx, r.Master.Cluster.States(), r.Job, ref, job.NewTaskStatus())
	if err := reporter.ReportFailure(Aborted); err != nil {
		return errors.Wrap(err, "abort")
	}

	jobWaitCtx, cancel := context.WithCancel(ctx)
	r.Master.JobTracker.OnJobCompletion(r.Job, func(*job.Job, *job.Status) {
		log.Info("Aborted {} successfully.", r.Job.ID)
		cancel()
	})
	<-jobWaitCtx.Done()
	return Aborted
}

func (r *RunningJob) dumpMetricsOnBackground() {
	go func() {
		metrics, err := r.Metrics()
		if err != nil {
			log.Warn("Collecting metric of job {} failed: {}", r.Job.ID, err)
			return
		}
		log.Verbose("Metrics collected:\n{}", metrics)
	}()
}
