package testutils

import (
	"context"
	"strconv"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/worker"
)

func StartLocalCluster(c C, numWorkers int) (sess *lrmr.Session, stopper func()) {
	crd := coordinator.NewLocalMemory()

	workers := make([]*worker.Worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		opt := worker.DefaultOptions()
		opt.ListenHost = "127.0.0.1:"
		opt.AdvertisedHost = "127.0.0.1:"
		opt.Concurrency = 2
		opt.NodeTags["No"] = strconv.Itoa(i + 1)

		w, err := worker.New(crd, opt)
		So(err, ShouldBeNil)
		w.SetWorkerLocalOption("No", i+1)
		w.SetWorkerLocalOption("IsWorker", true)

		go w.Start()
		workers[i] = w
	}

	// wait for workers to register themselves
	time.Sleep(200 * time.Millisecond)

	opt := master.DefaultOptions()
	opt.ListenHost = "127.0.0.1:"
	opt.AdvertisedHost = "127.0.0.1:"

	m, err := master.New(crd, opt)
	So(err, ShouldBeNil)
	m.Start()

	return lrmr.NewSession(context.Background(), m, lrmr.WithTimeout(time.Minute)), func() {
		for _, w := range workers {
			c.So(w.Close(), ShouldBeNil)
		}
		m.Stop()
	}
}
