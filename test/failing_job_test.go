package test

import (
	"testing"
	"time"

	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	"github.com/ab180/lrmr/worker"
	. "github.com/smartystreets/goconvey/convey"
)

func TestFailingJob(t *testing.T) {
	Convey("Running a job that fails", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := FailingJob(cluster.Session)

		Convey("It should handle errors gracefully on Wait", func() {
			job, err := ds.Run()
			So(err, ShouldBeNil)

			err = job.Wait()
			So(err, ShouldNotBeNil)
		})
		Convey("It should handle errors gracefully on Collect", func() {
			_, err := ds.Collect(testutils.ContextWithTimeout())
			So(err, ShouldNotBeNil)
		})
	}))
}

func TestFailingJob_WithFatalErrors(t *testing.T) {
	Convey("Running a job that fails with fatal errors", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := ComplicatedQuery(cluster.Session)

		Convey("It should handle errors gracefully on Wait", func() {
			job, err := ds.Run()
			So(err, ShouldBeNil)

			go forceKillWorker(cluster.Workers[0])

			err = job.Wait()
			So(err, ShouldNotBeNil)
		})
		Convey("It should handle errors gracefully on Collect", func() {
			go forceKillWorker(cluster.Workers[0])

			_, err := ds.Collect(testutils.ContextWithTimeout())
			So(err, ShouldNotBeNil)
		})
	}))
}

func forceKillWorker(w *worker.Worker) {
	time.Sleep(200 * time.Millisecond)

	// to emulate the case worker killed unexpectedly, let's just close RPC connections
	// while keeping the node registration
	w.RPCServer.Stop()
}
