package test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/test/testutils"
)

func TestComplicatedQuery(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		cluster := testutils.StartLocalCluster(c, 8)
		defer cluster.Stop()

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
	})
}
