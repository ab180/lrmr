package test

import (
	"testing"

	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSort(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When running Sort", func() {
			ds := Sort()

			Convey("It should sort given data", func() {
				rows, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
				So(err, ShouldBeNil)

				res := testutils.GroupRowsByKey(rows.Outputs)
				So(res, ShouldHaveLength, 3)

				So(res["foo"], ShouldHaveLength, 1)
				So(testutils.StringValue(res["foo"][0]), ShouldEqual, "6789")

				So(res["bar"], ShouldHaveLength, 1)
				So(testutils.StringValue(res["bar"][0]), ShouldEqual, "2345")

				So(res["baz"], ShouldHaveLength, 1)
				So(testutils.StringValue(res["baz"][0]), ShouldEqual, "1359")
			})
		})
	}))
}
