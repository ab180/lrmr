package worker

import (
	"context"
	"github.com/therne/lrmr/internal/serialization"
	"github.com/therne/lrmr/job"
)

type taskContext struct {
	context.Context
	worker    *Worker
	task      *job.Task
	stage     *job.Stage
	broadcast serialization.Broadcast
	cancel    context.CancelFunc
}

func newTaskContext(w *Worker, s *job.Stage, t *job.Task, b serialization.Broadcast) *taskContext {
	ctx, cancel := context.WithCancel(context.Background())
	return &taskContext{
		Context:   ctx,
		worker:    w,
		task:      t,
		broadcast: b,
		cancel:    cancel,
	}
}

func (c *taskContext) PartitionID() string {
	return c.task.PartitionID
}

func (c *taskContext) TotalPartitions() int {
	return len(c.stage.Partitions.Partitions)
}

func (c *taskContext) Broadcast(key string) interface{} {
	return c.broadcast[key]
}

func (c *taskContext) WorkerLocalValue(key string) interface{} {
	return c.worker.workerLocalOpts[key]
}

func (c *taskContext) Key() string {
	// TODO: provide key
	return c.task.PartitionID
}

func (c *taskContext) IncrementCounter(name string, delta float64) {
	c.worker.jobReporter.UpdateMetric(c.task.Reference(), func(metrics job.Metrics) {
		metrics[name] += int(delta)
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
