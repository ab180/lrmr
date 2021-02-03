package transformation

import (
	"context"

	"github.com/ab180/lrmr/job"
)

type Context interface {
	context.Context

	Broadcast(key string) interface{}
	WorkerLocalOption(key string) interface{}
	PartitionID() string
	Job() *job.Job

	AddMetric(name string, delta int)
	SetMetric(name string, val int)
}
