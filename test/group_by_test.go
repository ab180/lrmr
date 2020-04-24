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

func TestBasicGroupByKnownKeys_WithCollect(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		m, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When doing GroupBy", func() {
			ds := BasicGroupByKnownKeys(m)

			Convey("It should run without error", func() {
				res, err := ds.Collect(gocontext.TODO())
				So(err, ShouldBeNil)

				Convey("Its result should be collected", func() {
					Printf("Results: %v\n", res)
					So(res, ShouldHaveLength, 4)

					actualCount := 0
					So(func() { res["1737"][0].UnmarshalValue(&actualCount) }, ShouldNotPanic)
					So(actualCount, ShouldEqual, 179513)
				})
			})
		})
	})
}

func TestSimpleCount(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		m, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When doing Count operations", func() {
			ds := SimpleCount(m)
			j, err := ds.Run(gocontext.TODO(), "Test")
			So(err, ShouldBeNil)

			Convey("It should be run without error", func() {
				So(j.Wait(), ShouldBeNil)

				Convey("It should emit all metrics", func() {
					m, err := j.Metrics()
					So(err, ShouldBeNil)
					So(m, ShouldContainKey, "Count0/Events")
					So(m["Count0/Events"], ShouldEqual, 3)
				})
			})
		})
	})
}

func TestSimpleCount_WithCollect(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		m, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When doing Count operations", func() {
			ds := SimpleCount(m)

			Convey("Calling Collect() should return results with no error", func() {
				res, err := ds.Collect(gocontext.TODO())
				So(err, ShouldBeNil)
				So(res, ShouldHaveLength, 2)
				So(res["foo"], ShouldHaveLength, 1)
				So(res["bar"], ShouldHaveLength, 1)

				fooCount := 0
				So(func() { res["foo"][0].UnmarshalValue(&fooCount) }, ShouldNotPanic)
				So(fooCount, ShouldEqual, 2)
			})
		})
	})
}
