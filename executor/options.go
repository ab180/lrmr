package executor

import (
	"net"
	"runtime"

	"github.com/ab180/lrmr/output"
	"github.com/creasty/defaults"
)

type Options struct {
	ListenHost     string `default:"127.0.0.1:7466"`
	AdvertisedHost string `default:"127.0.0.1:7466"`

	// Concurrency is desired number of the executor threads in a executor.
	// By default, it will be number of CPUs in the machine.
	Concurrency int `default:"-"`

	// NodeTags is used for partitioner.
	NodeTags map[string]string `default:"{}"`

	// MaxMessageSize specifies the maximum message size in bytes the gRPC server can receive/send.
	// The default value is 500mb.
	MaxMessageSize int `default:"524288000"`

	Input struct {
		QueueLength int `default:"1000"`
	}
	Output output.Options

	ExperimentalCPUAffinity bool `default:"false"`

	// UseDelayedSendInput is a flag to use delayed send input.
	//
	// If this is enabled, the input will be buffered before sending it to the function. Result of buffering we can
	// check the exact execution time(performance) of the applying function without I/O latency.
	UseDelayedSendInput bool
}

func DefaultOptions() (o Options) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	o.SetDefaults()
	return
}

func (o *Options) SetDefaults() {
	if defaults.CanUpdate(o.Concurrency) {
		o.Concurrency = runtime.NumCPU()
	}
}

// WithOptions returns an executor with the given Options.
func WithOptions(o Options) func(e *Executor) {
	return func(e *Executor) {
		e.opt = o
	}
}

// WithListener returns an executor with the given net.Listener.
func WithListener(lis net.Listener) func(e *Executor) {
	return func(e *Executor) {
		e.serverLis = lis
	}
}
