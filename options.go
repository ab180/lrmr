package lrmr

import (
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/worker"
)

type Option func(op *Options)

type Options struct {
	EtcdEndpoints []string
	EtcdNamespace string

	NodeManager *node.ManagerOptions
	Master      MasterOptions
	Worker      *worker.Options
}

func DefaultOptions() *Options {
	return &Options{
		EtcdEndpoints: []string{"127.0.0.1:2379"},
		EtcdNamespace: "lrmr/",
		NodeManager:   node.DefaultManagerOptions(),
		Worker:        worker.DefaultOptions(),
		Master:        DefaultMasterOptions(),
	}
}

type MasterOptions struct {
	ListenHost     string
	AdvertisedHost string

	Output output.Options
}

func DefaultMasterOptions() MasterOptions {
	return MasterOptions{
		ListenHost:     "localhost:7600",
		AdvertisedHost: "localhost:7600",
		Output:         output.DefaultOptions(),
	}
}
