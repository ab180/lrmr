package lrmr

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/internal/util"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/metric"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
)

type RunningJob struct {
	*job.Job
	statusManager job.StatusManager
	finalStatus   atomic.Value
	statusMu      sync.RWMutex
	startedAt     time.Time
	logger        logger.Logger
}

func newRunningJob(j *job.Job, c cluster.State) *RunningJob {
	runningJob := &RunningJob{
		Job:           j,
		statusManager: job.NewDistributedStatusManager(c, j),
		startedAt:     time.Now(),
		logger:        logger.New(fmt.Sprintf("lrmr(%s)", j.ID)),
	}
	runningJob.statusManager.OnStageCompletion(func(stageName string, stageStatus *job.StageStatus) {
		runningJob.logger.Info("Stage {} {}.", stageName, stageStatus.Status)
	})
	runningJob.statusManager.OnJobCompletion(func(status *job.Status) {
		runningJob.logger.Info("Job {}. Total elapsed {}", status.Status, time.Since(j.SubmittedAt))

		groupTasksByError := make(map[string][]string)
		for _, errDesc := range status.Errors {
			simpleTaskID := strings.ReplaceAll(errDesc.Task, j.ID+"/", "")
			groupTasksByError[errDesc.Message] = append(groupTasksByError[errDesc.Message], simpleTaskID)
		}
		for errMsg, tasks := range groupTasksByError {
			runningJob.logger.Info(" - Error caused by [{}]: {}", strings.Join(tasks, ", "), errMsg)
		}
		runningJob.finalStatus.Store(status)
		err := runningJob.logMetrics()
		if err != nil {
			runningJob.logger.Warn("Failed to log metrics: {}", err)
		}
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

func (r *RunningJob) Metrics() (lrmrmetric.Metrics, error) {
	panic("not implemented")
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
	r.statusManager.OnJobCompletion(func(status *job.Status) {
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
		if err := r.AbortWithContext(context.Background()); err != nil {
			return errors.Wrap(err, "during abort")
		}
		return ctx.Err()
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
		StageName:   "master",
		PartitionID: "_",
	}
	if err := r.statusManager.Abort(ctx, ref); err != nil {
		return errors.Wrap(err, "abort")
	}

	wait := make(chan struct{})
	r.statusManager.OnJobCompletion(func(status *job.Status) {
		wait <- struct{}{}
	})
	<-wait
	return nil
}

func (r *RunningJob) logMetrics() error {
	jobDuration := time.Now().Sub(r.startedAt)
	lrmrmetric.JobDurationSummary.Observe(jobDuration.Seconds())
	lrmrmetric.RunningJobsGauge.Dec()
	metrics, err := r.Metrics()
	if err != nil {
		return err
	}
	r.logger.Info("Job metrics:\n{}", metrics.String())

	return nil
}
