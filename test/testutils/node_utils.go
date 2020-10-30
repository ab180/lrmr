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

type LocalCluster struct {
	Session *lrmr.Session

	crd     coordinator.Coordinator
	master  *master.Master
	workers []*worker.Worker
	testCtx C
}

func StartLocalCluster(c C, numWorkers int, options ...lrmr.SessionOption) *LocalCluster {
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

	options = append(options, lrmr.WithTimeout(30*time.Second))
	sess := lrmr.NewSession(context.Background(), m, options...)

	return &LocalCluster{
		Session: sess,
		crd:     crd,
		master:  m,
		workers: workers,
		testCtx: c,
	}
}

func (lc *LocalCluster) EmulateMasterFailure(old *lrmr.RunningJob) (new *lrmr.RunningJob) {
	lc.master.Stop()

	opt := master.DefaultOptions()
	opt.ListenHost = "127.0.0.1:"
	opt.AdvertisedHost = "127.0.0.1:"
	newMaster, err := master.New(lc.crd, opt)
	lc.testCtx.So(err, ShouldBeNil)

	newMaster.Start()
	newJob := &lrmr.RunningJob{
		Job:    old.Job,
		Master: newMaster,
	}
	newMaster.JobTracker.AddJob(old.Job)
	lc.master = newMaster

	return newJob
}

func (lc *LocalCluster) Stop() {
	for _, w := range lc.workers {
		lc.testCtx.So(w.Close(), ShouldBeNil)
	}
	lc.master.Stop()
}
