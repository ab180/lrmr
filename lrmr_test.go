package lrmr

import (
	"testing"

	"github.com/ab180/lrmr/cluster"

	"github.com/ab180/lrmr/coordinator"
	"github.com/stretchr/testify/require"
)

func TestConnectToCluster(t *testing.T) {
	crd := coordinator.NewLocalMemory()
	opt := cluster.Options{
		MaxMessageSize: 500,
	}

	clu, err := ConnectToCluster(
		WithCoordinator(crd),
		WithClusterOptions(opt),
	)
	if err != nil {
		t.Error(err)
	}

	require.Equal(t, crd, clu.coordinator)
	require.Equal(t, opt, clu.ClusterOptions)
}
