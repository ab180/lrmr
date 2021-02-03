package transformation

import (
	"context"
	"time"
)

type Context interface {
	context.Context

	Broadcast(key string) interface{}
	WorkerLocalOption(key string) interface{}
	PartitionID() string
	JobID() string
	JobSubmittedAt() time.Time

	AddMetric(name string, delta int)
	SetMetric(name string, val int)
}
