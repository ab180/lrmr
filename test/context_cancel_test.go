package test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ab180/lrmr/test/integration"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestContextCancel(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Running a job", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := ContextCancel(2 * time.Second)

		Convey("It should be cancelled after cancelling the Context", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			result, err := ds.RunAndCollect(ctx, cluster)
			So(err, ShouldBeNil)

			err = result.Err()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Expected DeadlineExceeded, got %v", err)
			}

			time.Sleep(500 * time.Millisecond)
			So(canceled.Load(), ShouldBeTrue)
		})
	}))
}

func TestContextCancel_WithinForLoop(t *testing.T) {
	defer goleak.VerifyNone(t)

	integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := ContextCancelWithInputLoop()

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		result, err := ds.RunAndCollect(ctx, cluster)
		require.Nil(t, err)

		err = result.Err()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Expected DeadlineExceeded, got %v", err)
		}
	})()
}

func TestContextCancel_WithLocalPipes(t *testing.T) {
	defer goleak.VerifyNone(t)

	Convey("Running a job", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := ContextCancelWithLocalPipe()

		Convey("It should be cancelled after cancelling the Context", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			result, err := ds.RunAndCollect(ctx, cluster)
			So(err, ShouldBeNil)

			err = result.Err()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("Expected DeadlineExceeded, got %v", err)
			}
		})
	}))
}
