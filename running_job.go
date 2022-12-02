package lrmr

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/driver"
	"github.com/ab180/lrmr/internal/errchannel"
	"github.com/ab180/lrmr/internal/util"
	"github.com/ab180/lrmr/job"
	lrmrmetric "github.com/ab180/lrmr/metric"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
)

// abortTimeout specifies how long it need to be waited for actual job abort.
const abortTimeout = 5 * time.Second

type RunningJob struct {
	*job.Job
	jobErrChan    *errchannel.ErrChannel
	driver        driver.Driver
	statusManager job.Manager
	finalStatus   atomic.Value
	startedAt     time.Time
	logger        logger.Logger
}

func startTrackingDetachedJob(j *job.Job, c cluster.State, drv driver.Driver) *RunningJob {
	runningJob := &RunningJob{
		Job:           j,
		jobErrChan:    errchannel.New(),
		driver:        drv,
		statusManager: job.NewDistributedManager(c, j),
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
			runningJob.jobErrChan.Send(status.Errors[0])
			return
		}
		runningJob.jobErrChan.Send(nil)
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
	case err := <-r.jobErrChan.Recv():
		return err

	case <-ctx.Done():
		log.Verbose("Context cancelled while waiting. Aborting...")

		abortCtx, abortCancel := context.WithTimeout(ctx, abortTimeout)
		defer abortCancel()

		if err := r.AbortWithContext(abortCtx); err != nil {
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

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.jobErrChan.Recv():
		return nil
	}
}

func (r *RunningJob) logMetrics() {
	jobDuration := time.Since(r.startedAt)
	lrmrmetric.JobDurationSummary.Observe(jobDuration.Seconds())
	lrmrmetric.RunningJobsGauge.Dec()
}

// AbortDetachedJob stops a job's execution.
func AbortDetachedJob(ctx context.Context, cluster cluster.Cluster, jobID string) error {
	ref := job.TaskID{
		JobID:       jobID,
		StageName:   "master",
		PartitionID: "_",
	}

	jobErrChan := errchannel.New()
	defer jobErrChan.Close()

	jobManager := job.NewDistributedManager(cluster.States(), &job.Job{ID: jobID})
	defer jobManager.Close()

	jobManager.OnJobCompletion(func(*job.Status) {
		jobErrChan.Send(nil)
	})
	if err := jobManager.Abort(ctx, ref); err != nil {
		return errors.Wrap(err, "abort")
	}

	// wait until the job to be cancelled
	for {
		select {
		case <-jobErrChan.Recv():
			return nil
		case <-ctx.Done():
			return ctx.Err()
		default:
			js, err := jobManager.FetchStatus(ctx)
			if err != nil {
				return err
			}

			if js.Status == job.Failed {
				return nil
			}

			log.Verbose("Waiting for job to be canceled...")

			time.Sleep(time.Second)
		}
	}
}
