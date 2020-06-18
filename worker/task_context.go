package worker

import (
	"context"

	"github.com/therne/lrmr/internal/serialization"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/transformation"
)

type taskContext struct {
	context.Context
	worker    *Worker
	task      *job.Task
	broadcast serialization.Broadcast
	cancel    context.CancelFunc
}

func newTaskContext(parent context.Context, w *Worker, t *job.Task, b serialization.Broadcast) *taskContext {
	ctx, cancel := context.WithCancel(parent)
	return &taskContext{
		Context:   ctx,
		worker:    w,
		task:      t,
		broadcast: b,
		cancel:    cancel,
	}
}

func (c taskContext) PartitionID() string {
	return c.task.PartitionID
}

func (c taskContext) Broadcast(key string) interface{} {
	return c.broadcast[key]
}

func (c taskContext) WorkerLocalOption(key string) interface{} {
	return c.worker.workerLocalOpts[key]
}

func (c *taskContext) AddMetric(name string, delta int) {
	c.worker.jobReporter.UpdateMetric(c.task.Reference(), func(metrics job.Metrics) {
		metrics[name] += int(delta)
	})
}

func (c *taskContext) SetMetric(name string, val int) {
	c.worker.jobReporter.UpdateMetric(c.task.Reference(), func(metrics job.Metrics) {
		metrics[name] = val
	})
}

func (c *taskContext) SetGauge(name string, val float64) {
	panic("implement me")
}

func (c *taskContext) ObserveHistogram(name string, val float64) {
	panic("implement me")
}

func (c *taskContext) ObserveSummary(name string, val float64) {
	panic("implement me")
}

// taskContext implements transformation.Context.
var _ transformation.Context = (*taskContext)(nil)
