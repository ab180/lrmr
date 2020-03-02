package worker

import (
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
)

type taskContext struct {
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
