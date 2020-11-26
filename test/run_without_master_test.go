package test

import (
	"testing"

	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
)

func TestRunWithoutMaster(t *testing.T) {
	Convey("When running a job", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ctx := testutils.ContextWithTimeout()

		job, err := RunWithoutMaster(cluster.Session).Run()
		So(err, ShouldBeNil)

		Convey("Stopping master should not affect job run", func() {
			newJob := cluster.EmulateMasterFailure(job)

			err := newJob.WaitWithContext(ctx)
			So(err, ShouldBeNil)

			Convey("Stopping master should not affect metric collection", func() {
				m, err := newJob.Metrics()
				So(err, ShouldBeNil)
				So(m["Input"], ShouldEqual, 5)
			})
		})
	}))
}
