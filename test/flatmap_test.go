package test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/test/integration"
	"github.com/therne/lrmr/test/testutils"
)

func TestFlatMap(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
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
	}))
}
