package lrmr

import (
	"time"

	"github.com/creasty/defaults"
)

type PipelineOptions struct {
	Name         string
	StartTimeout time.Duration `default:"5s"`
	NodeSelector map[string]string
}

type PipelineOption func(o *PipelineOptions)

func WithName(n string) PipelineOption {
	return func(o *PipelineOptions) {
		o.Name = n
	}
}

func WithStartTimeout(d time.Duration) PipelineOption {
	return func(o *PipelineOptions) {
		o.StartTimeout = d
	}
}

func WithNodeSelector(selector map[string]string) PipelineOption {
	return func(o *PipelineOptions) {
		o.NodeSelector = selector
	}
}

func buildSessionOptions(opts []PipelineOption) (o PipelineOptions) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	for _, optFn := range opts {
		optFn(&o)
	}
	return o
}
