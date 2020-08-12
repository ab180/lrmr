package master

import (
	"github.com/creasty/defaults"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
)

type Options struct {
	ListenHost     string `default:"localhost:7600"`
	AdvertisedHost string `default:"localhost:7600"`

	CollectQueueSize int `default:"1000"`

	RPC   node.ManagerOptions
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
