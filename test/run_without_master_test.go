package test

import (
	"context"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/test/testutils"
)

func TestRunWithoutMaster(t *testing.T) {
	Convey("When running a job", t, func(c C) {
		cluster := testutils.StartLocalCluster(c, 2)
		defer cluster.Stop()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		job, err := RunWithoutMaster(cluster.Session).Run()
		So(err, ShouldBeNil)

		Convey("Stopping master should not affect job run", func() {
			newJob := cluster.EmulateMasterFailure(job)

			err := newJob.WaitWithContext(ctx)
			So(err, ShouldBeNil)

			Convey("Stopping master should not affect metric collection", func() {
				m, err := newJob.Metrics()
				So(err, ShouldBeNil)
				So(m["HaltForMasterFailure0/Input"], ShouldEqual, 5)
			})
		})

	})
}
