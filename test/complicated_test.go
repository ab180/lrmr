package test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/test/integration"
	"go.uber.org/goleak"
)

func TestComplicatedQuery(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When doing ComplicatedQuery", func() {
			ds := ComplicatedQuery(cluster.Session)
			j, err := ds.Run()
			So(err, ShouldBeNil)

			Convey("It should be run without error", func() {
				So(j.Wait(), ShouldBeNil)

				Convey("It should emit all metrics", func() {
					m, err := j.Metrics()
					So(err, ShouldBeNil)
					So(m, ShouldResemble, job.Metrics{
						"Files":  55,
						"Events": 647437,
					})
				})
			})
		})
	}))
}
