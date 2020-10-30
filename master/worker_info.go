package master

import (
	"context"

	"github.com/therne/lrmr/cluster"
	"github.com/therne/lrmr/cluster/node"
	"google.golang.org/grpc"
)

type WorkerHolder struct {
	cluster cluster.Cluster
	*node.Node
}

func (w WorkerHolder) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	return w.cluster.Connect(ctx, w.Host)
}
