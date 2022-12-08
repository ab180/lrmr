package test

import (
	"context"
	"testing"

	"github.com/ab180/lrmr/test/integration"
	"github.com/stretchr/testify/require"
)

func TestMap(t *testing.T) {
	integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := Map()

		result, err := ds.RunAndCollect(context.Background(), cluster)
		require.Nil(t, err)

		rowLen := 0
		var max int32
		for row := range result.Outputs() {
			n := int32(*row.Value.(*int32Row))
			if n > max {
				max = n
			}

			rowLen++
		}
		err = result.Err()
		require.Nil(t, err)

		require.Equal(t, 1000, rowLen)
		require.Equal(t, int32(8000), max)
	})()
}
