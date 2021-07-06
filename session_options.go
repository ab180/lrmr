package lrmr

import (
	"time"

	"github.com/creasty/defaults"
)

type SessionOptions struct {
	Name         string
	StartTimeout time.Duration `default:"5s"`
	NodeSelector map[string]string
}

type SessionOption func(o *SessionOptions)

func WithName(n string) SessionOption {
	return func(o *SessionOptions) {
		o.Name = n
	}
}

func WithStartTimeout(d time.Duration) SessionOption {
	return func(o *SessionOptions) {
		o.StartTimeout = d
	}
}

func WithNodeSelector(selector map[string]string) SessionOption {
	return func(o *SessionOptions) {
		o.NodeSelector = selector
	}
}

func buildSessionOptions(opts []SessionOption) (o SessionOptions) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	for _, optFn := range opts {
		optFn(&o)
	}
	return o
}
