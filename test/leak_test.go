package test

import (
	"testing"

	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/goleak"
)

func TestLeakOnShortRunning(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When running short-running job", func() {
			ds := SimpleCount()

			Convey("It should not leak any goroutines", func() {
				result, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
				So(err, ShouldBeNil)

				// Flush returned rows.
				for _ = range result.Outputs() { //nolint:gosimple
				}
			})
		})
	}))
}
