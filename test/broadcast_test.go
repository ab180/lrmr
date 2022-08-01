package test

import (
	"testing"

	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
)

func TestBroadcast(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When running Map with broadcasts", func() {
			ds := BroadcastTester()

			Convey("It should run without preserving broadcast values from master", func() {
				result, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
				So(err, ShouldBeNil)

				var rows []*lrdd.Row
				for row := range result.Outputs() {
					rows = append(rows, row)
				}
				err = result.Err()
				So(err, ShouldBeNil)

				So(rows, ShouldHaveLength, 1)
				So(testutils.StringValue(rows[0]), ShouldEqual, "throughStruct=foo, throughContext=bar, typeMatched=true")
			})
		})
	}))
}
