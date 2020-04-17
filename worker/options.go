package worker

import (
	"github.com/creasty/defaults"
	"github.com/therne/lrmr/output"
	"runtime"
)

type Options struct {
	ListenHost     string `default:"127.0.0.1:7466"`
	AdvertisedHost string `default:"127.0.0.1:7466"`

	// Concurrency is desired number of the executor threads in a worker.
	// By default, it will be number of CPUs in the machine.
	Concurrency int `default:"-"`

	Input struct {
		QueueLength int `default:"1000"`
		MaxRecvSize int `default:"67108864"`
	}
	Output output.Options
}

func DefaultOptions() (o Options) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	return
}

func (o Options) SetDefaults() {
	o.Concurrency = runtime.NumCPU()
}
