package test

import (
	"context"
	"testing"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/test/integration"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestNodeSelection(t *testing.T) {
	integration.WithLocalCluster(
		2,
		func(cluster *integration.LocalCluster) {
			j, err := NodeSelection(map[string]string{"No": "1"}).
				RunInBackground(cluster)
			require.Nil(t, err)

			err = j.WaitWithContext(context.Background())
			require.Nil(t, err)

			m, err := j.Metrics()
			require.Nil(t, err)

			// NumPartitions = (number of nodes) * 2 (=default concurrency in StartLocalCluster)
			require.Equal(t, uint64(2), m["NumPartitions"])
		})()

	integration.WithLocalCluster(
		2,
		func(cluster *integration.LocalCluster) {
			_, err := NodeSelection(map[string]string{"going": "nowhere"}).
				RunInBackground(cluster)

			require.Equal(t, lrmr.ErrNoAvailableExecutors, errors.Cause(err))
		})()
}
