package executor

import (
	"context"
	"time"

	"github.com/ab180/lrmr/transformation"
)

type taskContext struct {
	context.Context
	executor *TaskExecutor
}

func newTaskContextWithCancel(ctx context.Context, executor *TaskExecutor) (*taskContext, context.CancelFunc) {
	c, cancel := context.WithCancel(ctx)
	return &taskContext{
		Context:  c,
		executor: executor,
	}, cancel
}

func (c taskContext) PartitionID() string {
	return c.executor.task.PartitionID
}

func (c taskContext) JobID() string {
	return c.executor.task.JobID
}

func (c taskContext) JobSubmittedAt() time.Time {
	return c.executor.job.Job.SubmittedAt
}

func (c taskContext) Broadcast(key string) interface{} {
	return c.executor.job.Broadcasts[key]
}

func (c taskContext) WorkerLocalOption(key string) interface{} {
	return c.executor.localOptions[key]
}

func (c *taskContext) AddMetric(name string, delta int) {
	c.executor.Metrics[name] = uint64(int(c.executor.Metrics[name]) + delta)
}

func (c *taskContext) SetMetric(name string, val int) {
	c.executor.Metrics[name] = uint64(val)
}

// taskContext implements transformation.Context.
var _ transformation.Context = (*taskContext)(nil)
