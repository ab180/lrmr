package test

import (
	"context"
	"runtime"
	"runtime/debug"
	"testing"

	lrmrmetric "github.com/ab180/lrmr/metric"
	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testdata"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func TestBasicGroupByKey(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When doing GroupBy", func() {
			ds := BasicGroupByKey()
			j, err := ds.RunInBackground(cluster)
			So(err, ShouldBeNil)

			Convey("It should be run without error", func() {
				So(j.WaitWithContext(testutils.ContextWithTimeout()), ShouldBeNil)

				Convey("It should emit all metrics", func() {
					m, err := j.Metrics()
					So(err, ShouldBeNil)
					So(m, ShouldResemble, lrmrmetric.Metrics{
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

	ds := BasicGroupByKey()
	allocSum := uint64(0)
	for n := 0; n < b.N; n++ {
		start := new(runtime.MemStats)
		runtime.ReadMemStats(start)

		if _, err := ds.RunAndCollect(context.TODO(), cluster); err != nil {
			b.Fatalf("Failed to collect: %v", err)
		}
		end := new(runtime.MemStats)
		runtime.ReadMemStats(end)

		allocSum += end.TotalAlloc - start.TotalAlloc
	}
	b.StopTimer()
	_ = cluster.Close()
	b.Logf("Total Memory Used: Average %dMiB", allocSum/uint64(b.N)/1024/1024)

	runtime.GC()
	debug.FreeOSMemory()

	final := new(runtime.MemStats)
	runtime.ReadMemStats(final)
	b.Logf("Memory left after test: Average %dMiB", final.HeapAlloc/1024/1024)
}

func TestBasicGroupByKnownKeys_WithCollect(t *testing.T) {
	integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := BasicGroupByKnownKeys()

		rows, err := ds.RunAndCollect(context.Background(), cluster)
		require.Nil(t, err)

		res := testutils.GroupRowsByKey(rows.Outputs())
		err = rows.Err()
		require.Nil(t, err)

		require.Equal(t, 4, len(res))
		require.Equal(t, 197206, testutils.IntValue(res["8263"][0]))
	})()
}

func TestSimpleCount(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When doing Count operations", func() {
			ds := SimpleCount()
			j, err := ds.RunInBackground(cluster)
			So(err, ShouldBeNil)

			Convey("It should be run without error", func() {
				So(j.WaitWithContext(testutils.ContextWithTimeout()), ShouldBeNil)

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
			ds := SimpleCount()

			Convey("Calling Collect() should return results with no error", func() {
				rows, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
				So(err, ShouldBeNil)

				res := testutils.GroupRowsByKey(rows.Outputs())
				err = rows.Err()
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

func TestGroupByWithPartitionsWithNoInput(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When there are partitions with no inputs", func() {
			ds := GroupByWithPartitionsWithNoInput()

			Convey("Calling Collect() should return results with no error", func() {
				res, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
				So(err, ShouldBeNil)

				rowsByKey := testutils.GroupRowsByKey(res.Outputs())
				err = res.Err()
				So(err, ShouldBeNil)

				So(rowsByKey, ShouldHaveLength, 1)
				So(rowsByKey["foo"], ShouldHaveLength, 1)
				So(testutils.IntValue(rowsByKey["foo"][0]), ShouldEqual, 1)
			})
		})
	}))
}
