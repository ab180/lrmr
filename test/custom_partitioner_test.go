package test

import (
	"testing"

	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
)

func TestCustomPartitioner(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When running stage with custom partitioner", func() {
			ds := PartitionerWithNodeAffinityTest()

			Convey("It should assign rows with its designated partitions and physical nodes", func() {
				result, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
				So(err, ShouldBeNil)

				res := testutils.GroupRowsByKey(result.Outputs)
				So(res, ShouldHaveLength, 2)
				So(res["1"], ShouldHaveLength, 2)
				So(res["2"], ShouldHaveLength, 2)

				So(testutils.StringValues(res["1"]), ShouldContain, "key1-1")
				So(testutils.StringValues(res["1"]), ShouldContain, "key1-2")
				So(testutils.StringValues(res["2"]), ShouldContain, "key2-1")
				So(testutils.StringValues(res["2"]), ShouldContain, "key2-2")
			})
		})
	}))
}
