package test

import (
	"testing"

	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testdata"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
)

func TestBasicGroupByKey(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When doing GroupBy", func() {
			ds := BasicGroupByKey(cluster.Session)
			j, err := ds.Run()
			So(err, ShouldBeNil)

			Convey("It should be run without error", func() {
				So(j.Wait(), ShouldBeNil)

				Convey("It should emit all metrics", func() {
					m, err := j.Metrics()
					So(err, ShouldBeNil)
					So(m, ShouldResemble, job.Metrics{
						"Files":  testdata.TotalFiles,
						"Events": testdata.TotalRows,
					})
				})
			})
		})
	}))
}

func TestBasicGroupByKnownKeys_WithCollect(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When doing GroupBy", func() {
			ds := BasicGroupByKnownKeys(cluster.Session)

			Convey("It should run without error", func() {
				rows, err := ds.Collect()
				res := testutils.GroupRowsByKey(rows)
				So(err, ShouldBeNil)

				Convey("Its result should be collected", func() {
					So(res, ShouldHaveLength, 4)
					So(testutils.IntValue(res["8263"][0]), ShouldEqual, 197206)
				})
			})
		})
	}))
}

func TestSimpleCount(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When doing Count operations", func() {
			ds := SimpleCount(cluster.Session)
			j, err := ds.Run()
			So(err, ShouldBeNil)

			Convey("It should be run without error", func() {
				So(j.Wait(), ShouldBeNil)

				Convey("It should emit all metrics", func() {
					m, err := j.Metrics()
					So(err, ShouldBeNil)
					So(m, ShouldContainKey, "Events")
					So(m["Events"], ShouldEqual, 3)
				})
			})
		})
	}))
}

func TestSimpleCount_WithCollect(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When doing Count operations", func() {
			ds := SimpleCount(cluster.Session)

			Convey("Calling Collect() should return results with no error", func() {
				rows, err := ds.Collect()
				res := testutils.GroupRowsByKey(rows)

				So(err, ShouldBeNil)
				So(res, ShouldHaveLength, 2)
				So(res["foo"], ShouldHaveLength, 1)
				So(res["bar"], ShouldHaveLength, 1)

				So(testutils.IntValue(res["foo"][0]), ShouldEqual, 2)
				So(testutils.IntValue(res["bar"][0]), ShouldEqual, 1)
			})
		})
	}))
}
