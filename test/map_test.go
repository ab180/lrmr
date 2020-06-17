package test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/test/testutils"
)

func TestMap(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		sess, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When running Map", func() {
			ds := Map(sess)

			Convey("It should run without error", func() {
				res, err := ds.Collect()
				So(err, ShouldBeNil)
				So(res, ShouldHaveLength, 1)
				So(res[""], ShouldHaveLength, 1000)

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
