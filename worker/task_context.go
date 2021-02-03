package worker

import (
	"context"

	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/transformation"
)

type taskContext struct {
	context.Context
	executor *TaskExecutor
}

func newTaskContext(ctx context.Context, executor *TaskExecutor) *taskContext {
	return &taskContext{
		Context:  ctx,
		executor: executor,
	}
}

func (c taskContext) PartitionID() string {
	return c.executor.task.PartitionID
}

func (c taskContext) JobID() string {
	return c.executor.task.JobID
}

func (c taskContext) Broadcast(key string) interface{} {
	return c.executor.broadcast[key]
}

func (c taskContext) WorkerLocalOption(key string) interface{} {
	return c.executor.localOptions[key]
}

func (c *taskContext) AddMetric(name string, delta int) {
	c.executor.taskReporter.UpdateMetric(func(metrics job.Metrics) {
		metrics[name] += int(delta)
	})
}

func (c *taskContext) SetMetric(name string, val int) {
	c.executor.taskReporter.UpdateMetric(func(metrics job.Metrics) {
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
