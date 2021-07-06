package job

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/coordinator"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
)

type TaskReporter struct {
	clusterState cluster.State

	task    TaskID
	job     *Job
	status  *TaskStatus
	flushMu sync.Mutex
	dirty   atomic.Bool
}

func NewTaskReporter(cs cluster.State, j *Job, task TaskID, s *TaskStatus) *TaskReporter {
	return &TaskReporter{
		clusterState: cs,
		task:         task,
		job:          j,
		status:       s,
	}
}

func (r *TaskReporter) UpdateStatus(mutator func(*TaskStatus)) {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	mutator(r.status)
	r.dirty.Store(true)
}

func (r *TaskReporter) UpdateMetric(mutator func(Metrics)) {
	r.UpdateStatus(func(ts *TaskStatus) { mutator(ts.Metrics) })
}

func (r *TaskReporter) ReportSuccess(ctx context.Context) error {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()

	r.status.Complete(Succeeded)

	txn := coordinator.NewTxn().
		Put(taskStatusKey(r.task), r.status).
		IncrementCounter(doneTasksInStageKey(r.task))

	res, err := r.clusterState.Commit(ctx, txn)
	if err != nil {
		return errors.Wrap(err, "write etcd")
	}
	elapsed := r.status.CompletedAt.Sub(r.status.SubmittedAt)
	log.Verbose("Task {} succeeded after {}", r.task, elapsed)

	r.checkForStageCompletion(ctx, int(res[1].Counter), 0)
	return nil
}

// ReportFailure marks the task as failed. If the error is non-nil, it's added to the error list of the job.
// Passing nil in error will only cancel the task.
func (r *TaskReporter) ReportFailure(ctx context.Context, err error) error {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()

	r.status.Complete(Failed)
	if err != nil {
		r.status.Error = err.Error()
	}

	txn := coordinator.NewTxn().
		Put(taskStatusKey(r.task), r.status).
		IncrementCounter(doneTasksInStageKey(r.task)).
		IncrementCounter(failedTasksInStageKey(r.task))

	if err != nil {
		errDesc := Error{
			Task:       r.task.String(),
			Message:    err.Error(),
			Stacktrace: fmt.Sprintf("%+v", err),
		}
		txn = txn.Put(jobErrorKey(r.task), errDesc)
	}
	res, etcdErr := r.clusterState.Commit(ctx, txn)
	if etcdErr != nil {
		return errors.Wrap(etcdErr, "write etcd")
	}
	elapsed := r.status.CompletedAt.Sub(r.status.SubmittedAt)
	switch err.(type) {
	case *logger.PanicError:
		panicErr := err.(*logger.PanicError)
		log.Error("Task {} failed after {} with {}", r.task, elapsed, panicErr.Pretty())
	default:
		log.Error("Task {} failed after {} with error: {}", r.task, elapsed, err)
	}

	r.checkForStageCompletion(ctx, int(res[1].Counter), int(res[2].Counter))
	return nil
}

func (r *TaskReporter) checkForStageCompletion(ctx context.Context, currentDoneTasks, currentFailedTasks int) {
	if currentFailedTasks == 1 {
		// to prevent race between workers, the failure is only reported by the first worker failed
		if err := r.reportStageCompletion(ctx, Failed); err != nil {
			log.Error("Failed to report completion of failed stage", err)
		}
		return
	}
	totalTasks := len(r.job.GetPartitionsOfStage(r.task.StageName))
	if currentDoneTasks == totalTasks {
		failedTasks, err := r.clusterState.ReadCounter(ctx, failedTasksInStageKey(r.task))
		if err != nil {
			log.Error("Failed to get count of failed stages of {}/{}", err, r.task.JobID, r.task.StageName)
			return
		}
		if failedTasks > 0 {
			return
		}
		if err := r.reportStageCompletion(ctx, Succeeded); err != nil {
			log.Error("Failed to report completion of succeeded stage", err)
		}
	}
}

func (r *TaskReporter) reportStageCompletion(ctx context.Context, status RunningState) error {
	log.Verbose("Reporting {} stage {}/{} (by {})", status, r.job.ID, r.task.StageName, r.task)

	var s StageStatus
	if err := r.clusterState.Get(ctx, stageStatusKey(r.job.ID, r.task.StageName), &s); err != nil {
		return errors.Wrap(err, "read stage status")
	}
	s.Complete(status)
	if err := r.clusterState.Put(ctx, stageStatusKey(r.job.ID, r.task.StageName), s); err != nil {
		return errors.Wrap(err, "update stage status")
	}
	if status == Failed {
		return r.reportJobCompletion(ctx, Failed)
	}

	doneStages, err := r.clusterState.IncrementCounter(ctx, doneStagesKey(r.job.ID))
	if err != nil {
		return errors.Wrap(err, "increment done stage count")
	}
	totalStages := int64(len(r.job.Stages)) - 1
	if doneStages == totalStages {
		return r.reportJobCompletion(ctx, Succeeded)
	}
	return nil
}

func (r *TaskReporter) reportJobCompletion(ctx context.Context, status RunningState) error {
	var js Status
	if err := r.clusterState.Get(ctx, jobStatusKey(r.job.ID), &js); err != nil {
		return errors.Wrapf(err, "get status of job %s", r.job.ID)
	}
	if js.Status == status {
		return nil
	}

	log.Verbose("Reporting {} job {} (by {})", status, r.job.ID, r.task)
	js.Complete(status)
	if err := r.clusterState.Put(ctx, jobStatusKey(r.job.ID), js); err != nil {
		return errors.Wrapf(err, "update status of job %s", r.job.ID)
	}
	return nil
}

func (r *TaskReporter) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	t := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-t.C:
			if err := r.flushTaskStatus(ctx); err != nil {
				log.Warn("Failed to report, will try again at next tick: {}", err)
			}
		case <-ctx.Done():
			if err := r.flushTaskStatus(ctx); err != nil {
				log.Warn("Failed to report, will try again at next tick: {}", err)
			}
			t.Stop()
			return
		}
	}
}

func (r *TaskReporter) flushTaskStatus(ctx context.Context) error {
	if !r.dirty.Load() {
		return nil
	}

	r.flushMu.Lock()
	status := r.status.Clone()
	r.flushMu.Unlock()

	return r.clusterState.Put(ctx, taskStatusKey(r.task), status)
}
