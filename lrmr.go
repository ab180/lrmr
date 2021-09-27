package lrmr

import (
	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/executor"
	"github.com/ab180/lrmr/lrdd"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
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
		return nil, errors.Wrap(err, "connect etcd")
	}
	clu, err := cluster.OpenRemote(etcd, opt.ClusterOptions)
	if err != nil {
		return nil, err
	}
	return &clusterWithEtcd{
		etcd:    etcd,
		Cluster: clu,
	}, nil
}

// clusterWithEtcd is for closing cluster with etcd.
type clusterWithEtcd struct {
	etcd coordinator.Coordinator
	cluster.Cluster
}

// Close closes etcd after closing cluster.
func (c *clusterWithEtcd) Close() error {
	if err := c.Cluster.Close(); err != nil {
		return err
	}
	return c.etcd.Close()
}

// NewExecutor creates a new Executor. Executor can run LRMR jobs on a distributed remote cluster.
func NewExecutor(c cluster.Cluster, optionalOpt ...executor.Options) (*executor.Executor, error) {
	opt := executor.DefaultOptions()
	if len(optionalOpt) > 0 {
		opt = optionalOpt[0]
	}
	return executor.New(c, opt)
}
