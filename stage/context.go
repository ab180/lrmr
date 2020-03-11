package stage

type Context interface {
	Broadcast(key string) interface{}
	WorkerLocalOption(key string) interface{}

	Spawn(func() error)
	PartitionKey() string

	AddMetric(name string, delta int)
	SetMetric(name string, val int)
}
