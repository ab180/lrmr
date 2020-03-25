package stage

type Context interface {
	Broadcast(key string) interface{}
	WorkerLocalOption(key string) interface{}
	PartitionKey() string

	AddMetric(name string, delta int)
	SetMetric(name string, val int)
}

type partitionKeyContext struct {
	Context
	partitionKey string
}

func WithPartitionKey(c Context, key string) Context {
	return &partitionKeyContext{
		Context:      c,
		partitionKey: key,
	}
}

func (pc partitionKeyContext) PartitionKey() string {
	return pc.partitionKey
}
