package test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/test/testutils"
)

func TestAssignTaskOnMaster(t *testing.T) {
	Convey("Given running nodes", t, func(c C) {
		sess, stop := testutils.StartLocalCluster(c, 2)
		defer stop()

		Convey("When assigning task on master", func() {
			ds := AssignTaskOnMaster(sess)

			Convey("It should be actually assigned on master without error", func() {
				rows, err := ds.Collect()
				So(err, ShouldBeNil)
				So(rows, ShouldHaveLength, 1)

				var tags []string
				rows[0].UnmarshalValue(&tags)
				So(tags, ShouldResemble, []string{"worker", "master"})
			})
		})
	})
}
