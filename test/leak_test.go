package test

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/test/integration"
	"go.uber.org/goleak"
)

func TestLeakOnShortRunning(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When running short-running job", func() {
			ds := AssignTaskOnMaster(cluster.Session)

			Convey("It should not leak any goroutines", func() {
				rows, err := ds.Collect()
				So(err, ShouldBeNil)
				So(rows, ShouldHaveLength, 1)
			})
		})
	}))
}
