package test

import (
	"testing"
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/test/integration"
	. "github.com/smartystreets/goconvey/convey"
)

func TestContextCancel(t *testing.T) {
	// defer goleak.VerifyNone(t)

	Convey("Running a job", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := ContextCancel(cluster.Session, time.Second)

		Convey("It should be cancelled after cancelling the Context", func() {
			_, err := ds.Collect()
			So(err, ShouldBeError, "job cancelled")
		})
	}, lrmr.WithTimeout(100*time.Millisecond)))
}
