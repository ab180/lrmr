package lrmr

import (
	"fmt"

	"github.com/ab180/lrmr/cluster"
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
func ConnectToCluster(options ...func(*Cluster)) (*Cluster, error) {
	lrmrCluster := &Cluster{
		ClusterOptions: defaultClusterOptions(),
	}
	for _, o := range options {
		o(lrmrCluster)
	}

	if lrmrCluster.coordinator == nil {
		crd, err := newDefaultCoordinator()
		if err != nil {
			return nil, fmt.Errorf("create new coordinator failed: %w", err)
		}

		lrmrCluster.coordinator = crd
	}

	clu, err := cluster.OpenRemote(lrmrCluster.coordinator, lrmrCluster.ClusterOptions)
	if err != nil {
		return nil, err
	}

	lrmrCluster.Cluster = clu

	return lrmrCluster, nil
}

// NewExecutor creates a new Executor. Executor can run LRMR jobs on a distributed remote cluster.
func NewExecutor(c cluster.Cluster, options ...func(*executor.Executor)) (*executor.Executor, error) {
	return executor.New(c, options...)
}
