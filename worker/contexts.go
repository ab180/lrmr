package worker

import (
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/stage"
)

type taskContext struct {
	worker    *Worker
	task      *job.Task
	broadcast map[string]interface{}
}

func NewTaskContext(w *Worker, t *job.Task, broadcast map[string]interface{}) *taskContext {
	return &taskContext{
		worker:    w,
		task:      t,
		broadcast: broadcast,
	}
}

func (c *taskContext) Broadcast(key string) interface{} {
	return c.broadcast[key]
}

func (c *taskContext) WorkerLocalOption(key string) interface{} {
	return c.worker.getWorkerLocalOption(key)
}

func (c *taskContext) PartitionKey() string {
	return c.task.PartitionKey
}

func (c *taskContext) AddMetric(name string, delta int) {
	c.worker.jobReporter.UpdateMetric(c.task.Reference(), func(metrics job.Metrics) {
		metrics[name] += delta
	})
}

func (c *taskContext) SetMetric(name string, val int) {
	c.worker.jobReporter.UpdateMetric(c.task.Reference(), func(metrics job.Metrics) {
		metrics[name] = val
	})
}

// taskContext implements stage.Context.
var _ stage.Context = &taskContext{}
