package lrmr

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"

	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/master"
	"github.com/ab180/lrmr/worker"
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

	etcd, err := coordinator.NewEtcd(opt.EtcdEndpoints, opt.EtcdNamespace)
	if err != nil {
		return nil, fmt.Errorf("connect etcd: %w", err)
	}
	return master.New(etcd, opt.Master)
}

func RunWorker(optionalOpt ...Options) error {
	runtime.GOMAXPROCS(runtime.NumCPU())

	opt := DefaultOptions()
	if len(optionalOpt) > 0 {
		opt = optionalOpt[0]
	}

	etcd, err := coordinator.NewEtcd(opt.EtcdEndpoints, opt.EtcdNamespace)
	if err != nil {
		return fmt.Errorf("connect etcd: %w", err)
	}
	w, err := worker.New(etcd, opt.Worker)
	if err != nil {
		return fmt.Errorf("init worker: %w", err)
	}
	go func() {
		if err := w.Start(); err != nil {
			log.Wtf("failed to start worker", err)
			return
		}
	}()

	waitForExit := make(chan os.Signal)
	signal.Notify(waitForExit, os.Interrupt, os.Kill)
	<-waitForExit

	if err := w.Close(); err != nil {
		log.Error("failed to shutdown historical node", err)
	}
	log.Info("Bye")
	return nil
}
