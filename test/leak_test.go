package test

import (
	"testing"
	"time"

	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/goleak"
)

func TestLeakOnShortRunning(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When running short-running job", func() {
			ds := ContextCancel(2 * time.Second)

			Convey("It should not leak any goroutines", func() {
				rows, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
				So(err, ShouldBeNil)
				So(rows, ShouldHaveLength, 1)
			})
		})
	}))
}
