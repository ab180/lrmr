package test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/test/integration"
)

func TestAssignTaskOnMaster(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When assigning task on master", func() {
			ds := AssignTaskOnMaster(cluster.Session)

			Convey("It should be actually assigned on master without error", func() {
				rows, err := ds.Collect()
				So(err, ShouldBeNil)
				So(rows, ShouldHaveLength, 1)

				var tags []string
				rows[0].UnmarshalValue(&tags)
				So(tags, ShouldResemble, []string{"worker", "master"})
			})
		})
	}))
}
