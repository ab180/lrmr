package master

import (
	"context"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/cluster/node"
	"google.golang.org/grpc"
)

type WorkerHolder struct {
	cluster cluster.Cluster
	*node.Node
}

func (w WorkerHolder) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	return w.cluster.Connect(ctx, w.Host)
}
