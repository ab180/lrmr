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
	"github.com/ab180/lrmr/lrmrmetric"
	"github.com/ab180/lrmr/master"
	"github.com/pkg/errors"
)

var (
	Aborted      = errors.New("job aborted")
	ErrCancelled = errors.New("job cancelled")
)

type RunningJob struct {
	*job.Job
	Master *master.Master

	tracker     *job.Tracker
	finalStatus atomic.Value
	statusMu    sync.RWMutex
	startedAt   time.Time
}

// TrackJob looks for an existing job and returns a RunningJob for tracking the job's lifecycle.
// coordinator.ErrNotFound is returned if given job ID does not exist.
func TrackJob(ctx context.Context, m *master.Master, jobID string) (*RunningJob, error) {
	j, err := m.JobManager.GetJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	return newRunningJob(m, j), nil
}

func newRunningJob(m *master.Master, j *job.Job) *RunningJob {
	runningJob := &RunningJob{
		Job:       j,
		tracker:   m.JobManager.Track(j),
		Master:    m,
		startedAt: time.Now(),
	}
	runningJob.tracker.OnStageCompletion(func(stageName string, stageStatus *job.StageStatus) {
		log.Verbose("Stage {}/{} {}.", j.ID, stageName, stageStatus.Status)
	})
	runningJob.tracker.OnJobCompletion(func(status *job.Status) {
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errChan := make(chan error, 1)
	r.tracker.OnJobCompletion(func(status *job.Status) {
		if status.Status == job.Failed {
			errChan <- status.Errors[0]
			return
		}
		errChan <- nil
	})

	select {
	case err := <-errChan:
		return err

	case <-ctx.Done():
		log.Info("Canceling jobs")
		_ = r.AbortWithContext(ctx)
		return ctx.Err()
	}
}

func (r *RunningJob) Collect() ([]*lrdd.Row, error) {
	return r.CollectWithContext(context.Background())
}

func (r *RunningJob) CollectWithContext(ctx context.Context) ([]*lrdd.Row, error) {
	collectResultsChan, err := r.Master.CollectedResults(r.Job.ID)
	if err != nil {
		return nil, err
	}

	errChan := make(chan error, 1)
	r.tracker.OnJobCompletion(func(status *job.Status) {
		if status.Status == job.Failed {
			errChan <- status.Errors[0]
			return
		}
		errChan <- nil
	})

	select {
	case err := <-errChan:
		select {
		case results := <-collectResultsChan:
			return results, err
		default:
			log.Warn("no output found during collect")
			return nil, err
		}

	case <-ctx.Done():
		log.Info("Canceling jobs")
		_ = r.AbortWithContext(ctx)
		return nil, ctx.Err()
	}
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
	reporter := job.NewTaskReporter(r.Master.Cluster.States(), r.Job, ref, job.NewTaskStatus())
	if err := reporter.ReportFailure(ctx, Aborted); err != nil {
		return errors.Wrap(err, "abort")
	}

	wait := make(chan struct{})
	r.tracker.OnJobCompletion(func(status *job.Status) {
		wait <- struct{}{}
	})
	<-wait

	log.Info("Aborted {} successfully.", r.Job.ID)
	return Aborted
}

func (r *RunningJob) logMetrics() {
	jobDuration := time.Now().Sub(r.startedAt)
	lrmrmetric.JobDurationSummary.Observe(jobDuration.Seconds())
	lrmrmetric.RunningJobsGauge.Dec()
}
