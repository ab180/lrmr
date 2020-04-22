package job

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/coordinator"
	"path"
	"sync"
	"time"
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

func (r *Reporter) UpdateStatus(ref TaskReference, mutator func(*TaskStatus)) {
	item, ok := r.statuses.Load(ref)
	if !ok {
		//tr.log.Warn("trying to update unknown task: {}", ref)
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
		r.log.Info("Task {} succeeded after {}", ref.String(), elapsed.String())
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

	// report full stacktrace
	errorKey := path.Join(jobErrorNs, ref.String())
	stacktrace := fmt.Sprintf("%+v", err)

	return r.crd.Batch(
		context.TODO(),
		coordinator.IncrementCounter(stageStatusKey(ref, "doneTasks")),
		coordinator.IncrementCounter(stageStatusKey(ref, "failedTasks")),
		coordinator.Put(errorKey, stacktrace),
	)
}

func (r *Reporter) ReportCancel(ref TaskReference) error {
	r.UpdateStatus(ref, func(ts *TaskStatus) {
		ts.Complete(Failed)
	})
	if err := r.flushTaskStatus(); err != nil {
		return fmt.Errorf("update task status: %w", err)
	}
	return r.crd.Batch(
		context.TODO(),
		coordinator.IncrementCounter(stageStatusKey(ref, "doneTasks")),
		coordinator.IncrementCounter(stageStatusKey(ref, "failedTasks")),
	)
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
	var updates []coordinator.BatchOp
	r.dirty.Range(func(key, value interface{}) bool {
		task := key.(TaskReference)
		isDirty := value.(bool)

		if isDirty {
			item, _ := r.statuses.Load(task)
			tsh := item.(*taskStatusHolder)

			tsh.lock.RLock()
			status := tsh.status.Clone()
			defer tsh.lock.RUnlock()

			key := path.Join(taskStatusNs, task.String())
			updates = append(updates, coordinator.Put(key, status))
		}
		return true
	})
	if len(updates) > 0 {
		if err := r.crd.Batch(r.ctx, updates...); err != nil {
			return err
		}
		// clear all dirties
		r.dirty.Range(func(key, value interface{}) bool {
			r.dirty.Store(key, false)
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

// jobStatusKey returns a key of job summary entry with given name.
func jobStatusKey(ref TaskReference, name string) string {
	return path.Join(jobStatusNs, ref.JobID, name)
}
