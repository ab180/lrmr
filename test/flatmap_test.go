package test

import (
	"testing"

	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
)

func TestFlatMap(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When running FlatMap", func() {
			ds := FlatMap()

			Convey("It should run without error", func() {
				res, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
				So(err, ShouldBeNil)

				max := 0
				for row := range res.Outputs() {
					n := testutils.IntValue(row)
					if n > max {
						max = n
					}
				}
				err = res.Err()
				So(err, ShouldBeNil)

				So(max, ShouldEqual, 8000)
			})
		})
	}))
}
