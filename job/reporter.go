package job

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/airbloc/logger"
	"github.com/therne/lrmr/coordinator"
)

type Reporter struct {
	crd coordinator.Coordinator

	statuses sync.Map
	dirty    sync.Map

	ctx    context.Context
	cancel context.CancelFunc
	log    logger.Logger
}

type taskStatusHolder struct {
	lock   sync.RWMutex
	status *TaskStatus
}

func NewJobReporter(crd coordinator.Coordinator) *Reporter {
	ctx, cancel := context.WithCancel(context.Background())
	return &Reporter{
		crd:    crd,
		ctx:    ctx,
		cancel: cancel,
		log:    logger.New("jobreporter"),
	}
}

func (r *Reporter) Add(task TaskReference, s *TaskStatus) {
	r.statuses.Store(task, &taskStatusHolder{status: s})
}

func (r *Reporter) Remove(task TaskReference) {
	r.statuses.Delete(task)
}

func (r *Reporter) UpdateStatus(ref TaskReference, mutator func(*TaskStatus)) {
	item, ok := r.statuses.Load(ref)
	if !ok {
		// tr.log.Warn("trying to update unknown task: {}", ref)
		return
	}
	tsh := item.(*taskStatusHolder)
	tsh.lock.Lock()
	mutator(tsh.status)
	tsh.lock.Unlock()

	if isDirty, ok := r.dirty.Load(ref); !ok || !isDirty.(bool) {
		r.dirty.Store(ref, true)
	}
}

func (r *Reporter) UpdateMetric(ref TaskReference, mutator func(Metrics)) {
	// todo: save metrics on another etcd key
	r.UpdateStatus(ref, func(ts *TaskStatus) { mutator(ts.Metrics) })
}

func (r *Reporter) ReportSuccess(ref TaskReference) error {
	r.UpdateStatus(ref, func(ts *TaskStatus) {
		ts.Complete(Succeeded)

		elapsed := ts.CompletedAt.Sub(ts.SubmittedAt)
		r.log.Verbose("Task {} succeeded after {}", ref.String(), elapsed.String())
	})
	if err := r.flushTaskStatus(); err != nil {
		return fmt.Errorf("update task status: %w", err)
	}

	ctx := context.TODO()
	if _, err := r.crd.IncrementCounter(ctx, stageStatusKey(ref, "doneTasks")); err != nil {
		return fmt.Errorf("increase done task count: %w", err)
	}
	return nil
}

func (r *Reporter) ReportFailure(ref TaskReference, err error) error {
	r.UpdateStatus(ref, func(ts *TaskStatus) {
		ts.Complete(Failed)
		ts.Error = err.Error()

		elapsed := ts.CompletedAt.Sub(ts.SubmittedAt)

		switch err.(type) {
		case *logger.PanicError:
			panicErr := err.(*logger.PanicError)
			r.log.Error("Task {} failed after {} with {}", ref.String(), elapsed.String(), panicErr.Pretty())
		default:
			r.log.Error("Task {} failed after {} with error: {}", ref.String(), elapsed.String(), err.Error())
		}
	})
	if err := r.flushTaskStatus(); err != nil {
		return fmt.Errorf("update task status: %w", err)
	}

	errDesc := Error{
		Task:       ref.String(),
		Message:    err.Error(),
		Stacktrace: fmt.Sprintf("%+v", err),
	}
	txn := coordinator.NewTxn().
		IncrementCounter(stageStatusKey(ref, "doneTasks")).
		IncrementCounter(stageStatusKey(ref, "failedTasks")).
		Put(jobErrorKey(ref), errDesc)

	return r.crd.Commit(context.TODO(), txn)
}

func (r *Reporter) ReportCancel(ref TaskReference) error {
	r.UpdateStatus(ref, func(ts *TaskStatus) {
		ts.Complete(Failed)
	})
	if err := r.flushTaskStatus(); err != nil {
		return fmt.Errorf("update task status: %w", err)
	}
	txn := coordinator.NewTxn().
		IncrementCounter(stageStatusKey(ref, "doneTasks")).
		IncrementCounter(stageStatusKey(ref, "failedTasks"))

	return r.crd.Commit(context.TODO(), txn)
}

func (r *Reporter) Start() {
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

func (r *Reporter) flushTaskStatus() error {
	txn := coordinator.NewTxn()
	r.dirty.Range(func(key, value interface{}) bool {
		task := key.(TaskReference)
		isDirty := value.(bool)

		if isDirty {
			item, ok := r.statuses.Load(task)
			if !ok {
				// probably the task is removed while waiting for flush
				r.dirty.Delete(key)
				return true
			}
			tsh := item.(*taskStatusHolder)

			tsh.lock.RLock()
			status := tsh.status.Clone()
			defer tsh.lock.RUnlock()

			txn.Put(path.Join(taskStatusNs, task.String()), status)
		}
		return true
	})
	if len(txn.Ops) > 0 {
		if err := r.crd.Commit(r.ctx, txn); err != nil {
			return err
		}
		// clear all dirties
		r.dirty.Range(func(key, value interface{}) bool {
			r.dirty.Delete(key)
			return true
		})
	}
	return nil
}

func (r *Reporter) Close() {
	r.cancel()
}

// stageStatusKey returns a key of stage summary entry with given name.
func stageStatusKey(ref TaskReference, name string) string {
	return path.Join(stageStatusNs, ref.JobID, ref.StageName, name)
}

// jobErrorKey returns a key of job error entry with given name.
func jobErrorKey(ref TaskReference) string {
	return path.Join(jobErrorNs, ref.String())
}
