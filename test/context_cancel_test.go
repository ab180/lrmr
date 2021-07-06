package test

import (
	"context"
	"testing"
	"time"

	"github.com/ab180/lrmr/test/integration"
	. "github.com/smartystreets/goconvey/convey"
)

func TestContextCancel(t *testing.T) {
	// defer goleak.VerifyNone(t)

	Convey("Running a job", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := ContextCancel(cluster.Session, time.Second)

		Convey("It should be cancelled after cancelling the Context", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := ds.Collect(ctx)
			So(err, ShouldBeError, context.DeadlineExceeded)
		})
	}))
}
