package lrmr

import (
	"fmt"
	"os"
	"os/signal"

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
func Parallelize(val interface{}) *Pipeline {
	return NewPipeline(&parallelizedInput{data: lrdd.From(val)})
}

// FromLocalFile creates new Pipeline, with reading files under given path an input.
func FromLocalFile(path string) *Pipeline {
	return NewPipeline(&localInput{Path: path})
}

// ConnectToCluster connects to remote cluster.
// A job sharing a same cluster object also shares gRPC connections to the executor nodes.
func ConnectToCluster(optionalOpt ...Options) (cluster.Cluster, error) {
	opt := DefaultOptions()
	if len(optionalOpt) > 0 {
		opt = optionalOpt[0]
	}

	etcd, err := coordinator.NewEtcd(opt.EtcdEndpoints, opt.EtcdNamespace, opt.EtcdOptions)
	if err != nil {
		return nil, fmt.Errorf("connect etcd: %w", err)
	}
	return cluster.OpenRemote(etcd, cluster.DefaultOptions())
}

// RunExecutor starts a new Executor. Executor can run tasks start
func RunExecutor(optionalOpt ...Options) error {
	opt := DefaultOptions()
	if len(optionalOpt) > 0 {
		opt = optionalOpt[0]
	}

	etcd, err := coordinator.NewEtcd(opt.EtcdEndpoints, opt.EtcdNamespace, opt.EtcdOptions)
	if err != nil {
		return fmt.Errorf("connect etcd: %w", err)
	}
	w, err := executor.New(etcd, opt.Executor)
	if err != nil {
		return fmt.Errorf("init executor: %w", err)
	}
	go func() {
		if err := w.Start(); err != nil {
			log.Wtf("failed to start executor", err)
			return
		}
	}()

	waitForExit := make(chan os.Signal)
	signal.Notify(waitForExit, os.Interrupt, os.Kill)
	<-waitForExit

	if err := w.Close(); err != nil {
		log.Error("failed to shutdown executor node", err)
	}
	log.Info("Bye")
	return nil
}
