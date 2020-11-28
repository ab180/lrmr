package lrmr

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ab180/lrmr/internal/util"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/master"
	"github.com/pkg/errors"
)

var (
	Aborted = errors.New("job aborted")
)

type RunningJob struct {
	*job.Job
	Master *master.Master

	jobContext  context.Context
	finalStatus atomic.Value
	statusMu    sync.RWMutex
}

// TrackJob looks for an existing job and returns a RunningJob for tracking the job's lifecycle.
// coordinator.ErrNotFound is returned if given job ID does not exist.
func TrackJob(ctx context.Context, m *master.Master, jobID string) (*RunningJob, error) {
	j, err := m.JobManager.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	tracker := m.JobManager.Track(ctx, j)
	return newRunningJob(m, j, tracker), nil
}

func newRunningJob(m *master.Master, j *job.Job, tracker *job.Tracker) *RunningJob {
	runningJob := &RunningJob{
		Job:        j,
		Master:     m,
		jobContext: tracker.JobContext(),
	}

	tracker.OnStageCompletion(func(stageName string, stageStatus *job.StageStatus) {
		log.Verbose("Stage {}/{} {}.", j.ID, stageName, stageStatus.Status)
	})
	tracker.OnJobCompletion(func(status *job.Status) {
		log.Info("Job {} {}. Total elapsed {}", j.ID, status.Status, time.Since(j.SubmittedAt))
		for i, errDesc := range status.Errors {
			log.Info(" - Error #{} caused by {}: {}", i, errDesc.Task, errDesc.Message)
		}
		runningJob.finalStatus.Store(status)
		runningJob.logMetrics()
	})
	return runningJob
}

func (r *RunningJob) Status() job.RunningState {
	status := r.finalStatus.Load()

	if status == nil {
		return job.Running
	}
	return status.(*job.Status).Status
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
	select {
	case <-r.jobContext.Done():
		status, err := r.Master.JobManager.GetJobStatus(ctx, r.Job.ID)
		if err != nil {
			return errors.Wrap(err, "load status of completed job")
		}
		if status.Status == job.Failed {
			return status.Errors[0]
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
	reporter := job.NewTaskReporter(ctx, r.Master.Cluster.States(), r.Job, ref, job.NewTaskStatus())
	if err := reporter.ReportFailure(Aborted); err != nil {
		return errors.Wrap(err, "abort")
	}

	<-r.jobContext.Done()
	log.Info("Aborted {} successfully.", r.Job.ID)
	return Aborted
}

func (r *RunningJob) logMetrics() {
	metrics, err := r.Metrics()
	if err != nil {
		log.Warn("Collecting metric of job {} failed: {}", r.Job.ID, err)
		return
	}
	log.Verbose("Metrics collected:\n{}", metrics)
}
