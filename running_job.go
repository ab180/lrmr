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
	"github.com/ab180/lrmr/driver"
	"github.com/ab180/lrmr/internal/util"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/metric"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
)

type RunningJob struct {
	*job.Job
	jobErrChan    chan error
	driver        driver.Driver
	statusManager job.StatusManager
	finalStatus   atomic.Value
	statusMu      sync.RWMutex
	startedAt     time.Time
	logger        logger.Logger
}

func startTrackBackgroundJob(j *job.Job, c cluster.State, drv driver.Driver) *RunningJob {
	runningJob := &RunningJob{
		Job:           j,
		jobErrChan:    make(chan error),
		driver:        drv,
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
		runningJob.logMetrics()

		if status.Status == job.Failed {
			runningJob.jobErrChan <- status.Errors[0]
			return
		}
		runningJob.jobErrChan <- nil
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
	return r.statusManager.CollectMetrics(context.Background())
}

func (r *RunningJob) Wait() error {
	ctx, cancel := util.ContextWithSignal(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)
	defer cancel()

	return r.WaitWithContext(ctx)
}

func (r *RunningJob) WaitWithContext(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	select {
	case err := <-r.jobErrChan:
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

	<-r.jobErrChan
	return nil
}

func (r *RunningJob) logMetrics() {
	jobDuration := time.Now().Sub(r.startedAt)
	lrmrmetric.JobDurationSummary.Observe(jobDuration.Seconds())
	lrmrmetric.RunningJobsGauge.Dec()
}
