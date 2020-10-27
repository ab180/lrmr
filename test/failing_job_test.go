package test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/test/testutils"
)

func TestFailingJob(t *testing.T) {
	Convey("Running a job that fails", t, func(c C) {
		cluster := testutils.StartLocalCluster(c, 2)
		defer cluster.Stop()

		ds := FailingJob(cluster.Session)

		Convey("It should handle errors gracefully on Wait", func() {
			job, err := ds.Run()
			So(err, ShouldBeNil)

			err = job.Wait()
			So(err, ShouldNotBeNil)
		})
		Convey("It should handle errors gracefully on Collect", func() {
			_, err := ds.Collect()
			So(err, ShouldNotBeNil)
		})
	})
}
