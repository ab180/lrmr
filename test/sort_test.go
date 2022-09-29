package test

import (
	"testing"

	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
)

func TestSort(t *testing.T) {
	Convey("Given running nodes", t, integration.WithLocalCluster(2, func(cluster *integration.LocalCluster) {
		Convey("When running Sort", func() {
			ds := Sort()

			Convey("It should sort given data", func() {
				rows, err := ds.RunAndCollect(testutils.ContextWithTimeout(), cluster)
				require.Nil(t, err)

				res := testutils.GroupRowsByKey(rows.Outputs())
				err = rows.Err()
				require.Nil(t, err)

				require.Equal(t, 3, len(res))

				require.Equal(t, 1, len(res["foo"]))
				require.Equal(t, "6789", string(res["foo"][0].Value))

				require.Equal(t, 1, len(res["bar"]))
				require.Equal(t, "2345", string(res["bar"][0].Value))

				require.Equal(t, 1, len(res["baz"]))
				require.Equal(t, "1359", string(res["baz"][0].Value))
			})
		})
	}))
}
