package transformation

type Context interface {
	Broadcast(key string) interface{}
	WorkerLocalOption(key string) interface{}
	NumExecutors() int
	CurrentExecutor() int

	AddTotalProgress(int)
	AddProgress(int)

	AddCustomMetric(name string, delta int)
	SetCustomMetric(name string, val int)
}
