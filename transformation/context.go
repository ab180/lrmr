package transformation

import "context"

type Context interface {
	context.Context

	Broadcast(key string) interface{}
	WorkerLocalOption(key string) interface{}
	PartitionID() string
	JobID() string

	AddMetric(name string, delta int)
	SetMetric(name string, val int)
}
