package integration

import (
	"strconv"
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/executor"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

type LocalCluster struct {
	cluster.Cluster
	crd       coordinator.Coordinator
	closeEtcd func()
	Executors []*executor.Executor
	testCtx   C
}

func NewLocalCluster(numWorkers int) (*LocalCluster, error) {
	workers := make([]*executor.Executor, numWorkers)
	etcd, closeEtcd := ProvideEtcd()
	c, err := cluster.OpenRemote(etcd, cluster.DefaultOptions())
	if err != nil {
		return nil, err
	}

	for i := 0; i < numWorkers; i++ {
		opt := executor.DefaultOptions()
		opt.ListenHost = "127.0.0.1:"
		opt.AdvertisedHost = "127.0.0.1:"
		opt.Concurrency = 2
		opt.NodeTags["No"] = strconv.Itoa(i + 1)

		w, err := executor.New(c, opt)
		if err != nil {
			return nil, errors.Wrapf(err, "init executor #%d", i)
		}
		w.SetWorkerLocalOption("No", i+1)
		w.SetWorkerLocalOption("IsWorker", true)

		go w.Start()
		workers[i] = w
	}

	// wait for workers to register themselves
	time.Sleep(200 * time.Millisecond)

	return &LocalCluster{
		Cluster:   c,
		closeEtcd: closeEtcd,
		crd:       etcd,
		Executors: workers,
	}, nil
}

func WithLocalCluster(numWorkers int, fn func(c *LocalCluster), options ...lrmr.PipelineOption) func() {
	return func() {
		c, err := NewLocalCluster(numWorkers)
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

func (lc *LocalCluster) Close() error {
	defer lc.closeEtcd()

	for i, w := range lc.Executors {
		if err := w.Close(); err != nil {
			return errors.Wrapf(err, "close executor #%d", i)
		}
	}
	return lc.Cluster.Close()
}
