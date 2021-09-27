package lrmr

import (
	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/coordinator"
	"github.com/creasty/defaults"
)

type ConnectClusterOptions struct {
	EtcdEndpoints  []string `default:"[\"127.0.0.1:2379\"]"`
	EtcdNamespace  string   `default:"lrmr/"`
	EtcdOptions    coordinator.EtcdOptions
	ClusterOptions cluster.Options
}

func DefaultConnectClusterOptions() (o ConnectClusterOptions) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	return
}
