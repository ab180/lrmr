package lrmr

import (
	"fmt"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/executor"
	"github.com/ab180/lrmr/lrdd"
	"github.com/airbloc/logger"
)

var (
	log = logger.New("lrmr")
)

// Parallelize creates new Pipeline with given value as an input.
func Parallelize(val interface{}, options ...PipelineOption) *Pipeline {
	return NewPipeline(&parallelizedInput{data: lrdd.From(val)}, options...)
}

// FromLocalFile creates new Pipeline, with reading files under given path an input.
func FromLocalFile(path string, options ...PipelineOption) *Pipeline {
	return NewPipeline(&localInput{Path: path}, options...)
}

// ConnectToCluster connects to remote cluster.
// A job sharing a same cluster object also shares gRPC connections to the executor nodes.
func ConnectToCluster(optionalOpt ...ConnectClusterOptions) (cluster.Cluster, error) {
	opt := DefaultConnectClusterOptions()
	if len(optionalOpt) > 0 {
		opt = optionalOpt[0]
	}

	etcd, err := coordinator.NewEtcd(opt.EtcdEndpoints, opt.EtcdNamespace, opt.EtcdOptions)
	if err != nil {
		return nil, fmt.Errorf("connect etcd: %w", err)
	}
	return cluster.OpenRemote(etcd, cluster.DefaultOptions())
}

// NewExecutor creates a new Executor. Executor can run LRMR jobs on a distributed remote cluster.
func NewExecutor(c cluster.Cluster, optionalOpt ...executor.Options) (*executor.Executor, error) {
	opt := executor.DefaultOptions()
	if len(optionalOpt) > 0 {
		opt = optionalOpt[0]
	}
	return executor.New(c, opt)
}
