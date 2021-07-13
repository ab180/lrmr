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
	logger  logger.Logger
}

func NewTaskReporter(cs cluster.State, j *Job, task TaskID, s *TaskStatus) *TaskReporter {
	return &TaskReporter{
		clusterState: cs,
		task:         task,
		job:          j,
		status:       s,
		logger:       logger.New(fmt.Sprintf("lrmr(%s)", j.ID)),
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
	r.logger.Verbose("Task {} succeeded after {}", r.task.WithoutJobID(), elapsed)

	return r.checkForStageCompletion(ctx, int(res[1].Counter), 0)
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
	if _, err := r.clusterState.Commit(ctx, txn); err != nil {
		return errors.Wrap(err, "write etcd")
	}
	elapsed := r.status.CompletedAt.Sub(r.status.SubmittedAt)
	switch err.(type) {
	case *logger.PanicError:
		panicErr := err.(*logger.PanicError)
		r.logger.Error("Task {} failed after {} with {}", r.task.WithoutJobID(), elapsed, panicErr.Pretty())
	default:
		r.logger.Error("Task {} failed after {} with error: {}", r.task.WithoutJobID(), elapsed, err)
	}
	return r.reportJobCompletion(ctx, Failed)
}

func (r *TaskReporter) checkForStageCompletion(ctx context.Context, currentDoneTasks, currentFailedTasks int) error {
	if currentFailedTasks == 1 {
		// to prevent race between workers, the failure is only reported by the first worker failed
		if err := r.reportStageCompletion(ctx, Failed); err != nil {
			return errors.Wrap(err, "report failure of stage")
		}
		return nil
	}
	totalTasks := len(r.job.GetPartitionsOfStage(r.task.StageName))
	if currentDoneTasks == totalTasks {
		failedTasks, err := r.clusterState.ReadCounter(ctx, failedTasksInStageKey(r.task))
		if err != nil {
			return errors.Wrap(err, "read count of failed stage")
		}
		if failedTasks > 0 {
			return nil
		}
		if err := r.reportStageCompletion(ctx, Succeeded); err != nil {
			return errors.Wrap(err, "report success of stage")
		}
	}
	return nil
}

func (r *TaskReporter) reportStageCompletion(ctx context.Context, status RunningState) error {
	r.logger.Verbose("Reporting {} stage {} (by #{})", status, r.task.StageName, r.task.PartitionID)

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

	r.logger.Verbose("Reporting job {} (by {})", status, r.task.WithoutJobID())
	js.Complete(status)
	if err := r.clusterState.Put(ctx, jobStatusKey(r.job.ID), js); err != nil {
		return errors.Wrapf(err, "update status of job %s", r.job.ID)
	}
	return nil
}

func (r *TaskReporter) Abort(ctx context.Context) error {
	var js Status
	if err := r.clusterState.Get(ctx, jobStatusKey(r.job.ID), &js); err != nil {
		return errors.Wrapf(err, "get status of job %s", r.job.ID)
	}
	if js.Status == Failed {
		return nil
	}
	js.Complete(Failed)

	errDesc := Error{
		Task:    r.task.String(),
		Message: "aborted",
	}
	txn := coordinator.NewTxn().
		Put(jobErrorKey(r.task), errDesc).
		Put(jobStatusKey(r.job.ID), js)

	if _, err := r.clusterState.Commit(ctx, txn); err != nil {
		return errors.Wrap(err, "write etcd")
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
			if ctx.Err() != nil {
				continue
			}
			if err := r.flushTaskStatus(ctx); err != nil {
				if errors.Cause(err) == context.Canceled {
					continue
				}
				r.logger.Warn("Failed to update status of task {}, and will try again at next tick: {}", r.task.WithoutJobID(), err)
			}
		case <-ctx.Done():
			// extend context
			if err := r.flushTaskStatus(context.Background()); err != nil {
				r.logger.Warn("Failed to flush task {}: {}", r.task.WithoutJobID(), err)
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
