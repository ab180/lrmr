package test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/test/testutils"
)

func TestComplicatedQuery(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		sess, stop := testutils.StartLocalCluster(c, 8)
		defer stop()

		Convey("When doing ComplicatedQuery", func() {
			ds := ComplicatedQuery(sess)
			j, err := ds.Run()
			So(err, ShouldBeNil)

			Convey("It should be run without error", func() {
				So(j.Wait(), ShouldBeNil)

				Convey("It should emit all metrics", func() {
					m, err := j.Metrics()
					So(err, ShouldBeNil)
					So(m, ShouldResemble, job.Metrics{
						"jsonDecoder0/Files": 55,
						"counter2/Events":    647437,
					})
				})
			})
		})
	})
}
