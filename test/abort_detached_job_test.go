package test

import (
	"testing"
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

func TestAbortDetachedJob(t *testing.T) {
	Convey("Calling lrmr.AbortDetachedJob", t, integration.WithLocalCluster(1, func(c *integration.LocalCluster) {
		Convey("When the job is normal detached job", func() {
			runningJob, err := ContextCancel(time.Second * 10).
				RunInBackground(c)
			So(err, ShouldBeNil)

			Convey("It should abort the job only with job ID", func() {
				err := lrmr.AbortDetachedJob(testutils.ContextWithTimeout(), c, runningJob.ID)
				So(err, ShouldBeNil)
			})
		})

		Convey("When the job does not exist", func() {
			jobID := "NotFoundNotFound"
			Convey("It should raise job.ErrNotFound", func() {
				err := lrmr.AbortDetachedJob(testutils.ContextWithTimeout(), c, jobID)
				So(errors.Cause(err), ShouldBeError, job.ErrNotFound)
			})
		})
	}))
}
