package stage

type Context interface {
	Broadcast(key string) interface{}
	WorkerLocalOption(key string) interface{}
	NumExecutors() int
	CurrentExecutor() int

	AddMetric(name string, delta int)
	SetMetric(name string, val int)
}
