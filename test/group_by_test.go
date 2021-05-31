package test

import (
	"runtime"
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

func BenchmarkBasicGroupByKey(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	cluster, err := integration.NewLocalCluster(2)
	if err != nil {
		b.Fatalf("Failed to start local cluster: %v", err)
	}
	b.StartTimer()

	ds := BasicGroupByKey(cluster.Session)
	allocSum := uint64(0)
	for n := 0; n < b.N; n++ {
		start := new(runtime.MemStats)
		runtime.ReadMemStats(start)

		if _, err := ds.Collect(); err != nil {
			b.Fatalf("Failed to collect: %v", err)
		}
		end := new(runtime.MemStats)
		runtime.ReadMemStats(end)

		allocSum += end.TotalAlloc - start.TotalAlloc
	}
	b.StopTimer()
	_ = cluster.Close()
	b.Logf("Total Memory Used: Average %dMiB", allocSum/uint64(b.N)/1024/1024)
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
