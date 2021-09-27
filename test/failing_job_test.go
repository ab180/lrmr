package test

import (
	"testing"
	"time"

	"github.com/ab180/lrmr/executor"
	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
)

func TestFailingJob(t *testing.T) {
	Convey("Running a job that fails", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := FailingJob()

		Convey("It should handle errors gracefully on Wait", func() {
			job, err := ds.RunInBackground(cluster)
			So(err, ShouldBeNil)

			err = job.WaitWithContext(testutils.ContextWithTimeout())
			So(err, ShouldNotBeNil)
		})
		Convey("It should handle errors gracefully on Collect", func() {
			_, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
			So(err, ShouldNotBeNil)
		})
	}))
}

func TestFailingJob_WithFatalErrors(t *testing.T) {
	Convey("Running a job that fails with fatal errors", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := ComplicatedQuery()

		Convey("It should handle errors gracefully on Wait", func() {
			job, err := ds.RunInBackground(cluster)
			So(err, ShouldBeNil)

			go forceKillWorker(cluster.Executors[0])

			err = job.WaitWithContext(testutils.ContextWithTimeout())
			So(err, ShouldNotBeNil)
		})
		Convey("It should handle errors gracefully on Collect", func() {
			go forceKillWorker(cluster.Executors[0])

			_, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
			So(err, ShouldNotBeNil)
		})
	}))
}

func forceKillWorker(w *executor.Executor) {
	time.Sleep(200 * time.Millisecond)

	// to emulate the case executor killed unexpectedly, let's just close RPC connections
	// while keeping the node registration
	w.RPCServer.Stop()
}
