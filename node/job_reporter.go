package node

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/internal/logutils"
	"path"
	"sync"
	"time"
)

type JobReporter struct {
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

func NewJobReporter(crd coordinator.Coordinator) *JobReporter {
	ctx, cancel := context.WithCancel(context.Background())
	return &JobReporter{
		crd:    crd,
		ctx:    ctx,
		cancel: cancel,
		log:    logger.New("task-reporter"),
	}
}

func (tr *JobReporter) Add(task TaskReference, s *TaskStatus) {
	tr.statuses.Store(task, &taskStatusHolder{status: s})
}

func (tr *JobReporter) UpdateStatus(ref TaskReference, mutator func(*TaskStatus)) {
	item, ok := tr.statuses.Load(ref)
	if !ok {
		//tr.log.Warn("trying to update unknown task: {}", ref)
		return
	}
	tsh := item.(*taskStatusHolder)
	tsh.lock.Lock()
	mutator(tsh.status)
	tsh.lock.Unlock()

	if isDirty, ok := tr.dirty.Load(ref); !ok || !isDirty.(bool) {
		tr.dirty.Store(ref, true)
	}
}

func (tr *JobReporter) UpdateMetric(ref TaskReference, mutator func(Metrics)) {
	// todo: save metrics on another etcd key
	tr.UpdateStatus(ref, func(ts *TaskStatus) { mutator(ts.Metrics) })
}

func (tr *JobReporter) ReportSuccess(ref TaskReference) error {
	tr.UpdateStatus(ref, func(ts *TaskStatus) {
		ts.Complete(Succeeded)

		elapsed := ts.CompletedAt.Sub(*ts.SubmittedAt)
		tr.log.Info("Task {} succeeded after {}", ref.String(), elapsed.String())
	})
	if err := tr.flushTaskStatus(); err != nil {
		return fmt.Errorf("update task status: %w", err)
	}

	ctx := context.TODO()
	if _, err := tr.crd.IncrementCounter(ctx, stageStatusKey(ref, "doneTasks")); err != nil {
		return fmt.Errorf("increase done task count: %w", err)
	}
	return nil
}

func (tr *JobReporter) ReportFailure(ref TaskReference, err error) error {
	tr.UpdateStatus(ref, func(ts *TaskStatus) {
		ts.Complete(Failed)
		ts.Error = err.Error()

		elapsed := ts.CompletedAt.Sub(*ts.SubmittedAt)

		switch err.(type) {
		case *logutils.PanicError:
			panicErr := err.(*logutils.PanicError)
			tr.log.Error("Task {} failed after {} with {}", ref.String(), elapsed.String(), panicErr.Pretty())
		default:
			tr.log.Error("Task {} failed after {} with error: {}", ref.String(), elapsed.String(), err.Error())
		}
	})
	if err := tr.flushTaskStatus(); err != nil {
		return fmt.Errorf("update task status: %w", err)
	}

	return tr.crd.Batch(
		context.TODO(),
		coordinator.IncrementCounter(stageStatusKey(ref, "doneTasks")),
		coordinator.IncrementCounter(stageStatusKey(ref, "failedTasks")),
	)
}

func (tr *JobReporter) Start() {
	go func() {
		t := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-t.C:
				if err := tr.flushTaskStatus(); err != nil {
					tr.log.Warn("Failed to report, will try again at next tick: {}", err)
				}
			case <-tr.ctx.Done():
				t.Stop()
				return
			}
		}
	}()
}

func (tr *JobReporter) flushTaskStatus() error {
	var updates []coordinator.BatchOp
	tr.dirty.Range(func(key, value interface{}) bool {
		task := key.(TaskReference)
		isDirty := value.(bool)

		if isDirty {
			item, _ := tr.statuses.Load(task)
			tsh := item.(*taskStatusHolder)

			tsh.lock.RLock()
			defer tsh.lock.RUnlock()

			key := path.Join(taskStatusNs, task.String())
			updates = append(updates, coordinator.Put(key, tsh.status))
		}
		return true
	})
	if len(updates) > 0 {
		if err := tr.crd.Batch(tr.ctx, updates...); err != nil {
			return err
		}
		// clear all dirties
		tr.dirty.Range(func(key, value interface{}) bool {
			tr.dirty.Store(key, false)
			return true
		})
	}
	return nil
}

func (tr *JobReporter) Close() {
	tr.cancel()
}

// stageStatusKey returns a key of stage summary entry with given name.
func stageStatusKey(ref TaskReference, name string) string {
	return path.Join(stageStatusNs, ref.JobID, ref.StageName, name)
}

// jobStatusKey returns a key of job summary entry with given name.
func jobStatusKey(ref TaskReference, name string) string {
	return path.Join(jobStatusNs, ref.JobID, name)
}
