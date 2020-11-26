package master

import (
	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/output"
	"github.com/creasty/defaults"
)

type Options struct {
	ListenHost     string `default:"localhost:7600"`
	AdvertisedHost string `default:"localhost:7600"`

	CollectQueueSize int `default:"1000"`

	RPC   cluster.Options
	Input struct {
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

type CreateJobOptions struct {
	NodeSelector map[string]string
}

type CreateJobOption func(o *CreateJobOptions)

func WithNodeSelector(ns map[string]string) CreateJobOption {
	return func(o *CreateJobOptions) {
		o.NodeSelector = ns
	}
}

func buildCreateJobOptions(opts []CreateJobOption) (o CreateJobOptions) {
	for _, optFn := range opts {
		optFn(&o)
	}
	return o
}
