package job

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/cluster"
	"github.com/therne/lrmr/coordinator"
	"go.uber.org/atomic"
)

type TaskReporter struct {
	clusterState cluster.State

	task    TaskID
	job     *Job
	status  *TaskStatus
	flushMu sync.Mutex
	dirty   atomic.Bool

	ctx context.Context
	log logger.Logger
}

func NewTaskReporter(ctx context.Context, cs cluster.State, j *Job, task TaskID, s *TaskStatus) *TaskReporter {
	return &TaskReporter{
		clusterState: cs,
		task:         task,
		job:          j,
		status:       s,
		ctx:          ctx,
		log:          logger.New("lrmr.jobReporter"),
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

func (r *TaskReporter) ReportSuccess() error {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()

	r.status.Complete(Succeeded)

	txn := coordinator.NewTxn().
		Put(path.Join(taskStatusNs, r.task.String()), r.status).
		IncrementCounter(stageStatusKey(r.task, "doneTasks"))

	res, err := r.clusterState.Commit(r.ctx, txn)
	if err != nil {
		return errors.Wrap(err, "write etcd")
	}
	elapsed := r.status.CompletedAt.Sub(r.status.SubmittedAt)
	r.log.Verbose("Task {} succeeded after {}", r.task, elapsed)

	r.checkForStageCompletion(int(res[1].Counter), 0)
	return nil
}

// ReportFailure marks the task as failed. If the error is non-nil, it's added to the error list of the job.
// Passing nil in error will only cancel the task.
func (r *TaskReporter) ReportFailure(err error) error {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()

	r.status.Complete(Failed)
	if err != nil {
		r.status.Error = err.Error()
	}

	txn := coordinator.NewTxn().
		Put(path.Join(taskStatusNs, r.task.String()), r.status).
		IncrementCounter(stageStatusKey(r.task, "doneTasks")).
		IncrementCounter(stageStatusKey(r.task, "failedTasks"))

	if err != nil {
		errDesc := Error{
			Task:       r.task.String(),
			Message:    err.Error(),
			Stacktrace: fmt.Sprintf("%+v", err),
		}
		txn = txn.Put(jobErrorKey(r.task), errDesc)
	}
	res, etcdErr := r.clusterState.Commit(r.ctx, txn)
	if etcdErr != nil {
		return errors.Wrap(etcdErr, "write etcd")
	}
	elapsed := r.status.CompletedAt.Sub(r.status.SubmittedAt)
	switch err.(type) {
	case *logger.PanicError:
		panicErr := err.(*logger.PanicError)
		r.log.Error("Task {} failed after {} with {}", r.task, elapsed, panicErr.Pretty())
	default:
		r.log.Error("Task {} failed after {} with error: {}", r.task, elapsed, err)
	}

	r.checkForStageCompletion(int(res[1].Counter), int(res[2].Counter))
	return nil
}

func (r *TaskReporter) checkForStageCompletion(currentDoneTasks, currentFailedTasks int) {
	if currentFailedTasks == 1 {
		// to prevent race between workers, the failure is only reported by the first worker failed
		if err := r.reportStageCompletion(Failed); err != nil {
			r.log.Error("Failed to report completion of failed stage", err)
		}
		return
	}
	totalTasks := len(r.job.GetPartitionsOfStage(r.task.StageName))
	if currentDoneTasks == totalTasks {
		failedTasks, err := r.clusterState.ReadCounter(r.ctx, stageStatusKey(r.task, "failedTasks"))
		if err != nil {
			r.log.Error("Failed to get count of failed stages of {}/{}", err, r.task.JobID, r.task.StageName)
			return
		}
		if failedTasks > 0 {
			return
		}
		if err := r.reportStageCompletion(Succeeded); err != nil {
			r.log.Error("Failed to report completion of succeeded stage", err)
		}
	}
}

func (r *TaskReporter) reportStageCompletion(status RunningState) error {
	r.log.Verbose("Reporting {} stage {}/{} (by {})", status, r.job.ID, r.task.StageName, r.task)

	var s StageStatus
	if err := r.clusterState.Get(r.ctx, path.Join(stageStatusNs, r.job.ID, r.task.StageName), &s); err != nil {
		return errors.Wrap(err, "read stage status")
	}
	s.Complete(status)
	if err := r.clusterState.Put(r.ctx, path.Join(stageStatusNs, r.job.ID, r.task.StageName), s); err != nil {
		return errors.Wrap(err, "update stage status")
	}
	if status == Failed {
		return r.reportJobCompletion(Failed)
	}

	doneStagesKey := path.Join(jobStatusNs, r.job.ID, "doneStages")
	doneStages, err := r.clusterState.IncrementCounter(r.ctx, doneStagesKey)
	if err != nil {
		return errors.Wrap(err, "increment done stage count")
	}
	totalStages := int64(len(r.job.Stages)) - 1
	if doneStages == totalStages {
		return r.reportJobCompletion(Succeeded)
	}
	return nil
}

func (r *TaskReporter) reportJobCompletion(status RunningState) error {
	var js Status
	if err := r.clusterState.Get(r.ctx, path.Join(jobStatusNs, r.job.ID), &js); err != nil {
		return errors.Wrapf(err, "get status of job %s", r.job.ID)
	}
	if js.Status == status {
		return nil
	}

	r.log.Verbose("Reporting {} job {} (by {})", status, r.job.ID, r.task)
	js.Complete(status)
	if err := r.clusterState.Put(r.ctx, path.Join(jobStatusNs, r.job.ID), js); err != nil {
		return errors.Wrapf(err, "update status of job %s", r.job.ID)
	}
	return nil
}

func (r *TaskReporter) Start() {
	go func() {
		t := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-t.C:
				if err := r.flushTaskStatus(); err != nil {
					r.log.Warn("Failed to report, will try again at next tick: {}", err)
				}
			case <-r.ctx.Done():
				t.Stop()
				return
			}
		}
	}()
}

func (r *TaskReporter) flushTaskStatus() error {
	if !r.dirty.Load() {
		return nil
	}

	r.flushMu.Lock()
	status := r.status.Clone()
	r.flushMu.Unlock()

	return r.clusterState.Put(r.ctx, path.Join(taskStatusNs, r.task.String()), status)
}

// stageStatusKey returns a key of stage summary entry with given name.
func stageStatusKey(ref TaskID, name ...string) string {
	frags := []string{stageStatusNs, ref.JobID, ref.StageName}
	return path.Join(append(frags, name...)...)
}

// jobErrorKey returns a key of job error entry with given name.
func jobErrorKey(ref TaskID) string {
	return path.Join(jobErrorNs, ref.String())
}
