package testutils

import (
	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/worker"
	"time"
)

func StartLocalCluster(c C, numWorkers int) (m *master.Master, stopper func()) {
	crd := coordinator.NewLocalMemory()

	workers := make([]*worker.Worker, numWorkers)
	for i := 0; i < numWorkers; i++ {
		opt := worker.DefaultOptions()
		opt.ListenHost = "127.0.0.1:"
		opt.AdvertisedHost = "127.0.0.1:"
		opt.Concurrency = 2

		w, err := worker.New(crd, opt)
		So(err, ShouldBeNil)

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

	return m, func() {
		for _, w := range workers {
			c.So(w.Stop(), ShouldBeNil)
		}
		m.Stop()
	}
}
