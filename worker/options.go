package worker

import (
	"github.com/therne/lrmr/output"
)

type Options struct {
	Bind string
	Host string
	Port int

	PoolSize    int
	QueueLength int
	MaxRecvSize int

	Output output.Options
}

func DefaultOptions() *Options {
	return &Options{
		Bind:        "0.0.0.0",
		Host:        "127.0.0.1:7466",
		Port:        7466,
		PoolSize:    4,
		QueueLength: 1000,
		MaxRecvSize: 32 * 1024 * 1024,
		Output:      output.DefaultOptions(),
	}
}
