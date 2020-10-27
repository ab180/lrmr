package test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/test/testutils"
)

func TestFlatMap(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		cluster := testutils.StartLocalCluster(c, 2)
		defer cluster.Stop()

		Convey("When running FlatMap", func() {
			ds := FlatMap(cluster.Session)

			Convey("It should run without error", func() {
				rows, err := ds.Collect()
				So(err, ShouldBeNil)
				So(rows, ShouldHaveLength, 8000)

				max := 0
				for _, row := range rows {
					n := testutils.IntValue(row)
					if n > max {
						max = n
					}
				}
				So(max, ShouldEqual, 8000)
			})
		})
	})
}
