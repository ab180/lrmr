package test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/test/testutils"
)

func TestBroadcast(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		sess, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When running Map with broadcasts", func() {
			ds := BroadcastTester(sess)

			Convey("It should run without preserving broadcast values from master", func() {
				res, err := ds.Collect()
				So(err, ShouldBeNil)
				So(res[""], ShouldHaveLength, 1)
				So(testutils.StringValue(res[""][0]), ShouldEqual, "throughStruct=foo, throughContext=bar")
			})
		})
	})
}
