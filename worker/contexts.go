package worker

import (
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
)

type taskContext struct {
	worker *Worker

	job         *job.Job
	stage       *job.Stage
	task        *job.Task
	runner      stage.Runner
	shards      *output.Shards
	executors   *executorPool
	connections []*Connection

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
	return len(w.executors.Executors)
}

func (w *taskContext) CurrentExecutor() int {
	return 0
}

func (w *taskContext) AddMetric(name string, delta int) {
	w.worker.jobReporter.UpdateMetric(w.task.Reference(), func(metrics job.Metrics) {
		metrics[name] += delta
	})
}

func (w *taskContext) SetMetric(name string, val int) {
	w.worker.jobReporter.UpdateMetric(w.task.Reference(), func(metrics job.Metrics) {
		metrics[name] = val
	})
}

func (w *taskContext) addConnection(c *Connection) {
	w.connections = append(w.connections, c)
}

func (w *taskContext) forkForExecutor(executorID int) stage.Context {
	return &taskContextForExecutor{
		taskContext: w,
		executorID:  executorID,
	}
}

type taskContextForExecutor struct {
	*taskContext
	executorID int

	metric job.Metrics
}

func (tce *taskContextForExecutor) CurrentExecutor() int {
	return tce.executorID
}
