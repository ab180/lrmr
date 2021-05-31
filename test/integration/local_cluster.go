package integration

import (
	"context"
	"strconv"
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/master"
	"github.com/ab180/lrmr/worker"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

type LocalCluster struct {
	Session *lrmr.Session

	crd     coordinator.Coordinator
	master  *master.Master
	Workers []*worker.Worker
	testCtx C
}

func NewLocalCluster(numWorkers int, options ...lrmr.SessionOption) (*LocalCluster, error) {
	workers := make([]*worker.Worker, numWorkers)
	crd := ProvideEtcd()

	for i := 0; i < numWorkers; i++ {
		opt := worker.DefaultOptions()
		opt.ListenHost = "127.0.0.1:"
		opt.AdvertisedHost = "127.0.0.1:"
		opt.Concurrency = 2
		opt.NodeTags["No"] = strconv.Itoa(i + 1)

		w, err := worker.New(crd, opt)
		if err != nil {
			return nil, errors.Wrapf(err, "init worker #%d", i)
		}
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
	if err != nil {
		return nil, errors.Wrap(err, "init master")
	}
	m.Start()

	options = append(options, lrmr.WithTimeout(30*time.Second))
	sess := lrmr.NewSession(context.Background(), m, options...)

	return &LocalCluster{
		Session: sess,
		crd:     crd,
		master:  m,
		Workers: workers,
	}, nil
}

func WithLocalCluster(numWorkers int, fn func(c *LocalCluster), options ...lrmr.SessionOption) func() {
	return func() {
		c, err := NewLocalCluster(numWorkers, options...)
		if err != nil {
			So(err, ShouldBeNil)
		}
		Reset(func() {
			if err := c.Close(); err != nil {
				So(err, ShouldBeNil)
			}
		})
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
	newJob, err := lrmr.TrackJob(context.Background(), newMaster, old.Job.ID)
	if err != nil {
		lc.testCtx.So(err, ShouldBeNil)
	}
	return newJob
}

func (lc *LocalCluster) Close() error {
	for i, w := range lc.Workers {
		if err := w.Close(); err != nil {
			return errors.Wrapf(err, "close worker #%d", i)
		}
	}
	lc.master.Stop()
	return nil
}
