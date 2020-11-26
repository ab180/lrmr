package integration

import (
	"context"
	"strconv"
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/master"
	"github.com/ab180/lrmr/worker"
	. "github.com/smartystreets/goconvey/convey"
)

type LocalCluster struct {
	Session *lrmr.Session

	crd     coordinator.Coordinator
	master  *master.Master
	workers []*worker.Worker
	testCtx C
}

func WithLocalCluster(numWorkers int, fn func(c *LocalCluster), options ...lrmr.SessionOption) func() {
	return func() {
		var m *master.Master
		workers := make([]*worker.Worker, numWorkers)
		Reset(func() {
			for _, w := range workers {
				So(w.Close(), ShouldBeNil)
			}
			m.Stop()
		})

		crd := ProvideEtcd()

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

		var err error
		m, err = master.New(crd, opt)
		So(err, ShouldBeNil)
		m.Start()

		options = append(options, lrmr.WithTimeout(30*time.Second))
		sess := lrmr.NewSession(context.Background(), m, options...)

		c := &LocalCluster{
			Session: sess,
			crd:     crd,
			master:  m,
			workers: workers,
		}

		fn(c)
	}
}

func (lc *LocalCluster) EmulateMasterFailure(old *lrmr.RunningJob) (new *lrmr.RunningJob) {
	lc.master.Stop()

	opt := master.DefaultOptions()
	opt.ListenHost = "127.0.0.1:"
	opt.AdvertisedHost = "127.0.0.1:"
	newMaster, err := master.New(lc.crd, opt)
	if err != nil {
		log.Error("Failed to create new master", err)
	}

	newMaster.Start()
	newJob := &lrmr.RunningJob{
		Job:    old.Job,
		Master: newMaster,
	}
	newMaster.JobTracker.AddJob(old.Job)
	lc.master = newMaster

	return newJob
}
