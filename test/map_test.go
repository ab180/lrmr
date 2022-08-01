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
			ds := Map()

			Convey("It should run without error", func() {
				result, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
				So(err, ShouldBeNil)

				rowLen := 0
				max := 0
				for row := range result.Outputs() {
					n := testutils.IntValue(row)
					if n > max {
						max = n
					}

					rowLen++
				}
				So(rowLen, ShouldEqual, 1000)

				err = result.Err()
				So(err, ShouldBeNil)

				So(max, ShouldEqual, 8000)
			})
		})
	}))
}
