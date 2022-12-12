package test

import (
	"context"
	"testing"

	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	"github.com/stretchr/testify/require"
)

func TestBroadcast(t *testing.T) {
	integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		ds := BroadcastTester()

		result, err := ds.RunAndCollect(context.Background(), cluster)
		require.Nil(t, err)

		var rows []lrdd.Row
		for row := range result.Outputs() {
			rows = append(rows, row)
		}
		err = result.Err()
		require.Nil(t, err)

		require.Equal(t, 1, len(rows))
		require.Equal(t, "throughStruct=foo, throughContext=bar, typeMatched=true", testutils.StringValue(rows[0]))
	})()
}
