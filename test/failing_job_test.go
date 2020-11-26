package test

import (
	"testing"

	"github.com/ab180/lrmr/test/integration"
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
			_, err := ds.Collect()
			So(err, ShouldNotBeNil)
		})
	}))
}
