package test

import (
	"testing"

	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
)

func TestMap(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When running Map", func() {
			ds := Map(cluster.Session)

			Convey("It should run without error", func() {
				rows, err := ds.Collect(testutils.ContextWithTimeout())
				So(err, ShouldBeNil)
				So(rows, ShouldHaveLength, 1000)

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
