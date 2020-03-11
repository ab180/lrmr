package worker

import (
	"github.com/therne/lrmr/job"
)

type executorContext struct {
	*runningTask
	partitionKey string
}

func (c *executorContext) Broadcast(key string) interface{} {
	return c.broadcasts["Broadcast/"+key]
}

func (c *executorContext) WorkerLocalOption(key string) interface{} {
	return c.worker.getWorkerLocalOption(key)
}

func (c *executorContext) PartitionKey() string {
	return c.partitionKey
}

func (c *executorContext) Spawn(fn func() error) {
	c.pool.JobQueue <- func() {
		defer c.TryRecover()
		if err := fn(); err != nil {
			_ = c.Abort(err)
		}
	}
}

func (c *executorContext) AddMetric(name string, delta int) {
	c.worker.jobReporter.UpdateMetric(c.task.Reference(), func(metrics job.Metrics) {
		metrics[name] += delta
	})
}

func (c *executorContext) SetMetric(name string, val int) {
	c.worker.jobReporter.UpdateMetric(c.task.Reference(), func(metrics job.Metrics) {
		metrics[name] = val
	})
}
