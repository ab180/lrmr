package lrmr

import (
	"github.com/creasty/defaults"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/worker"
)

type Option func(op *Options)

type Options struct {
	EtcdEndpoints []string `default:"[\"127.0.0.1:2379\"]"`
	EtcdNamespace string   `default:"lrmr/"`

	Master master.Options
	Worker worker.Options
}

func DefaultOptions() (o Options) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	return
}
