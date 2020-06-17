package test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/test/testutils"
)

func TestBasicGroupByKey(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		sess, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When doing GroupBy", func() {
			ds := BasicGroupByKey(sess)
			j, err := ds.Run()
			So(err, ShouldBeNil)

			Convey("It should be run without error", func() {
				So(j.Wait(), ShouldBeNil)

				Convey("It should emit all metrics", func() {
					m, err := j.Metrics()
					So(err, ShouldBeNil)
					So(m, ShouldResemble, job.Metrics{
						"jsonDecoder0/Files": 55,
						"counter1/Events":    647437,
					})
				})
			})
		})
	})
}

func TestBasicGroupByKnownKeys_WithCollect(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		sess, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When doing GroupBy", func() {
			ds := BasicGroupByKnownKeys(sess)

			Convey("It should run without error", func() {
				res, err := ds.Collect()
				So(err, ShouldBeNil)

				Convey("Its result should be collected", func() {
					So(res, ShouldHaveLength, 4)
					So(testutils.IntValue(res["1737"][0]), ShouldEqual, 179513)
				})
			})
		})
	})
}

func TestSimpleCount(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		sess, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When doing Count operations", func() {
			ds := SimpleCount(sess)
			j, err := ds.Run()
			So(err, ShouldBeNil)

			Convey("It should be run without error", func() {
				So(j.Wait(), ShouldBeNil)

				Convey("It should emit all metrics", func() {
					m, err := j.Metrics()
					So(err, ShouldBeNil)
					So(m, ShouldContainKey, "counter0/Events")
					So(m["counter0/Events"], ShouldEqual, 3)
				})
			})
		})
	})
}

func TestSimpleCount_WithCollect(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		sess, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When doing Count operations", func() {
			ds := SimpleCount(sess)

			Convey("Calling Collect() should return results with no error", func() {
				res, err := ds.Collect()
				So(err, ShouldBeNil)
				So(res, ShouldHaveLength, 2)
				So(res["foo"], ShouldHaveLength, 1)
				So(res["bar"], ShouldHaveLength, 1)

				So(testutils.IntValue(res["foo"][0]), ShouldEqual, 2)
				So(testutils.IntValue(res["bar"][0]), ShouldEqual, 1)
			})
		})
	})
}
