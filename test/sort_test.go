package test

import (
	gocontext "context"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/test/testutils"
	"testing"
)

func TestSort(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		m, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When running Sort", func() {
			ds := Sort(m)

			Convey("It should sort given data", func() {
				res, err := ds.Collect(gocontext.TODO())
				So(err, ShouldBeNil)
				So(res, ShouldHaveLength, 3)

				So(res["foo"], ShouldHaveLength, 1)
				So(testutils.StringValue(res["foo"][0]), ShouldEqual, "6789")

				So(res["bar"], ShouldHaveLength, 1)
				So(testutils.StringValue(res["bar"][0]), ShouldEqual, "2345")

				So(res["baz"], ShouldHaveLength, 1)
				So(testutils.StringValue(res["baz"][0]), ShouldEqual, "1359")
			})
		})
	})
}
