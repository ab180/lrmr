package lrmr

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/executor"
	"github.com/ab180/lrmr/lrdd"
	"github.com/airbloc/logger"
)

var (
	log = logger.New("lrmr")
)

func RunMaster(optionalOpt ...Options) (*master.Master, error) {
	opt := DefaultOptions()
	if len(optionalOpt) > 0 {
		opt = optionalOpt[0]
	}

	etcd, err := coordinator.NewEtcd(opt.EtcdEndpoints, opt.EtcdNamespace, opt.EtcdOptions)
	if err != nil {
		return nil, fmt.Errorf("connect etcd: %w", err)
	}
	return master.New(etcd, opt.Master)
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
	w, err := executor.New(etcd, opt.Worker)
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
