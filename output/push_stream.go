package output

import (
	"context"
	"io"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/cluster"
	"github.com/therne/lrmr/cluster/node"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"google.golang.org/grpc/metadata"
)

type PushStream struct {
	stream lrmrpb.Node_PushDataClient
	conn   io.Closer
}

func OpenPushStream(ctx context.Context, cluster cluster.Cluster, n *node.Node, host, taskID string) (*PushStream, error) {
	conn, err := cluster.Connect(ctx, host)
	if err != nil {
		return nil, errors.Wrapf(err, "connect %s", host)
	}

	header := &lrmrpb.DataHeader{
		TaskID: taskID,
	}
	if n != nil {
		header.FromHost = n.Host
	} else {
		header.FromHost = "master"
	}
	rawHead, _ := jsoniter.MarshalToString(header)
	runCtx := metadata.AppendToOutgoingContext(ctx, "dataHeader", rawHead)

	worker := lrmrpb.NewNodeClient(conn)
	stream, err := worker.PushData(runCtx)
	if err != nil {
		return nil, errors.Wrapf(err, "open stream to %s", host)
	}
	return &PushStream{
		stream: stream,
		conn:   conn,
	}, nil
}

func (p *PushStream) Write(data ...*lrdd.Row) (err error) {
	return p.stream.Send(&lrmrpb.PushDataRequest{Data: data})
}

func (p *PushStream) Close() error {
	return p.stream.CloseSend()
}
