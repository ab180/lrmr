package worker

import (
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
)

type taskContext struct {
	worker *Worker

	job            *node.Job
	stage          *node.Stage
	task           *node.Task
	transformation transformation.Transformation
	output         output.Output
	executors      *executorPool

	finishedInputCounts int32
	totalInputCounts    int32
	isRunning           bool

	states     map[string]interface{}
	broadcasts map[string]interface{}
	workerCfgs map[string]interface{}
}

func (w *taskContext) Broadcast(key string) interface{} {
	return w.broadcasts["Broadcast/"+key]
}

func (w *taskContext) WorkerLocalOption(key string) interface{} {
	return w.workerCfgs[key]
}

func (w *taskContext) NumExecutors() int {
	return len(w.executors.inputChans)
}

func (w *taskContext) CurrentExecutor() int {
	return 0
}

func (w *taskContext) AddTotalProgress(incremented int) {
	w.worker.jobReporter.UpdateStatus(w.task.Reference(), func(ts *node.TaskStatus) {
		ts.TotalProgress += uint64(incremented)
	})
}

func (w *taskContext) AddProgress(incremented int) {
	w.worker.jobReporter.UpdateStatus(w.task.Reference(), func(ts *node.TaskStatus) {
		ts.CurrentProgress += uint64(incremented)
	})
}

func (w *taskContext) forkForExecutor(executorID int) transformation.Context {
	return &taskContextForExecutor{
		taskContext: w,
		executorID:  executorID,
	}
}

type taskContextForExecutor struct {
	*taskContext
	executorID int
}
