package test

import (
	gocontext "context"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/test/testutils"
	"testing"
)

func TestBasicGroupByKey(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		m, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When doing GroupBy", func() {
			ds := BasicGroupByKey(m)
			j, err := ds.Run(gocontext.TODO(), "Test")
			So(err, ShouldBeNil)

			Convey("It should be run without error", func() {
				So(j.Wait(), ShouldBeNil)

				Convey("It should emit all metrics", func() {
					m, err := j.Metrics()
					So(err, ShouldBeNil)
					So(m, ShouldContainKey, "DecodeJSON0/Files")
					So(m, ShouldContainKey, "Count1/Events")
					So(m["DecodeJSON0/Files"], ShouldEqual, 55)
					So(m["Count1/Events"], ShouldEqual, 647437)
				})
			})
		})
	})
}

func TestBasicGroupByKnownKeys(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		m, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When doing GroupBy", func() {
			ds := BasicGroupByKnownKeys(m)
			j, err := ds.Run(gocontext.TODO(), "Test")
			So(err, ShouldBeNil)

			Convey("It should be run without error", func() {
				So(j.Wait(), ShouldBeNil)

				Convey("It should emit all metrics", func() {
					m, err := j.Metrics()
					So(err, ShouldBeNil)
					So(m, ShouldContainKey, "DecodeJSON0/Files")
					So(m, ShouldContainKey, "Count1/Events")
					So(m["DecodeJSON0/Files"], ShouldEqual, 55)
					So(m["Count1/Events"], ShouldEqual, 571949)
				})
			})
		})
	})
}
