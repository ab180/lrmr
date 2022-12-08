package test

import (
	"context"
	"testing"

	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := Sort()

		rows, err := ds.RunAndCollect(context.Background(), cluster)
		require.Nil(t, err)

		res := testutils.GroupRowsByKey(rows.Outputs())
		err = rows.Err()
		require.Nil(t, err)

		require.Equal(t, 3, len(res))

		require.Equal(t, 1, len(res["foo"]))
		require.Equal(t, "6789", string(*res["foo"][0].Value.(*lrdd.Bytes)))

		require.Equal(t, 1, len(res["bar"]))
		require.Equal(t, "2345", string(*res["bar"][0].Value.(*lrdd.Bytes)))

		require.Equal(t, 1, len(res["baz"]))
		require.Equal(t, "1359", string(*res["baz"][0].Value.(*lrdd.Bytes)))
	})()
}
