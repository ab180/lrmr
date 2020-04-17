package lrmr

import (
	"github.com/creasty/defaults"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/worker"
)

type Option func(op *Options)

type Options struct {
	EtcdEndpoints []string `default:"[\"127.0.0.1:2379\"]"`
	EtcdNamespace string   `default:"lrmr/"`

	NodeManager node.ManagerOptions
	Master      MasterOptions
	Worker      worker.Options
}

func DefaultOptions() (o Options) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	return
}

type MasterOptions struct {
	ListenHost     string `default:"localhost:7600"`
	AdvertisedHost string `default:"localhost:7600"`

	Output output.Options
}

func DefaultMasterOptions() (o MasterOptions) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	return
}
