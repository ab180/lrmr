package lrmr

import (
	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/master"
	"github.com/ab180/lrmr/worker"
	"github.com/creasty/defaults"
)

type Option func(op *Options)

type Options struct {
	EtcdEndpoints []string `default:"[\"127.0.0.1:2379\"]"`
	EtcdNamespace string   `default:"lrmr/"`
	EtcdOptions   coordinator.EtcdOptions

	Master master.Options
	Worker worker.Options
}

func DefaultOptions() (o Options) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	return
}
