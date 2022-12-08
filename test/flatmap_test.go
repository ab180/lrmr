package test

import (
	"context"
	"testing"

	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	"github.com/stretchr/testify/require"
)

func TestFlatMap(t *testing.T) {
	integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {

		ds := FlatMap()

		res, err := ds.RunAndCollect(context.Background(), cluster)
		require.Nil(t, err)

		max := 0
		for row := range res.Outputs() {
			n := testutils.IntValue(row)
			if n > max {
				max = n
			}
		}
		err = res.Err()
		require.Nil(t, err)

		require.Equal(t, 8000, max)
	})()
}
