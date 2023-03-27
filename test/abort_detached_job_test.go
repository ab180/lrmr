package test

import (
	"context"
	"testing"
	"time"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/test/integration"
	"github.com/stretchr/testify/require"
)

func TestAbortDetachedJob(t *testing.T) {
	c, err := integration.NewLocalCluster(1)
	require.Nil(t, err)
	defer func() {
		err := c.Close()
		require.Nil(t, err)
	}()

	// When the job is normal detached job.
	runningJob, err := ContextCancel(time.Second * 10).RunInBackground(c)
	require.Nil(t, err)

	// It should abort the job only with job ID.
	err = lrmr.AbortDetachedJob(context.Background(), c, runningJob.ID)
	require.Nil(t, err)

	// When the job does not exist, it should raise job.ErrNotFound.
	jobID := "NotFoundNotFound"
	err = lrmr.AbortDetachedJob(context.Background(), c, jobID)
	require.ErrorIs(t, err, job.ErrNotFound)
}
