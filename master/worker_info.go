package master

import (
	"context"

	"github.com/therne/lrmr/cluster/node"
	"google.golang.org/grpc"
)

type WorkerHolder struct {
	nm node.Manager
	*node.Node
}

func (w WorkerHolder) Connect(ctx context.Context) (*grpc.ClientConn, error) {
	return w.nm.Connect(ctx, w.Host)
}
