package output

import (
	"context"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"google.golang.org/grpc/metadata"
	"io"
)

type PushStream struct {
	stream lrmrpb.Worker_PushDataClient
	conn   io.Closer
}

func NewPushStream(ctx context.Context, nm node.Manager, host, taskID string) (*PushStream, error) {
	conn, err := nm.Connect(ctx, host)
	if err != nil {
		return nil, errors.Wrapf(err, "connect %s", host)
	}

	header := &lrmrpb.DataHeader{
		TaskID: taskID,
	}
	if nm.Self() != nil {
		header.FromHost = nm.Self().Host
	} else {
		header.FromHost = "master"
	}
	rawHead, _ := jsoniter.MarshalToString(header)
	runCtx := metadata.AppendToOutgoingContext(context.Background(), "dataHeader", rawHead)

	worker := lrmrpb.NewWorkerClient(conn)
	stream, err := worker.PushData(runCtx)
	if err != nil {
		return nil, errors.Wrapf(err, "open stream to %s", host)
	}
	return &PushStream{
		stream: stream,
		conn:   conn,
	}, nil
}

func (p *PushStream) Write(data []*lrdd.Row) (err error) {
	return p.stream.Send(&lrmrpb.PushDataRequest{Data: data})
}

func (p *PushStream) Close() error {
	return p.stream.CloseSend()
}
