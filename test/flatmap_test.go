package test

import (
	gocontext "context"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/test/testutils"
	"testing"
)

func TestFlatMap(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		m, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When running FlatMap", func() {
			ds := FlatMap(m)

			Convey("It should run without error", func() {
				res, err := ds.Collect(gocontext.TODO())
				So(err, ShouldBeNil)
				So(res, ShouldHaveLength, 1)
				So(res[""], ShouldHaveLength, 8000)

				max := 0
				for _, row := range res[""] {
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
