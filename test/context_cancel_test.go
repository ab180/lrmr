package test

import (
	"context"
	"testing"
	"time"

	"github.com/ab180/lrmr/test/integration"
	. "github.com/smartystreets/goconvey/convey"
	"go.uber.org/goleak"
)

func TestContextCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Running a job", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := ContextCancel(cluster.Session, time.Second)

		Convey("It should be cancelled after cancelling the Context", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := ds.Collect(ctx)
			So(err, ShouldBeError, context.DeadlineExceeded)

			time.Sleep(50 * time.Millisecond)
			So(canceled.Load(), ShouldBeTrue)
		})
	}))
}

func TestContextCancel_WithinForLoop(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Running a job", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := ContextCancelWithInputLoop(cluster.Session, time.Second)

		Convey("It should be cancelled after cancelling the Context", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			_, err := ds.Collect(ctx)
			So(err, ShouldBeError, context.DeadlineExceeded)
		})
	}))
}
