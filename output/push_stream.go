package output

import (
	"context"

	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/lrmrpb"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"google.golang.org/grpc/metadata"
)

type PushStream struct {
	stream   lrmrpb.Node_PushDataClient
	reqCache *lrmrpb.PushDataRequest
}

func OpenPushStream(ctx context.Context, rpc lrmrpb.NodeClient, n *node.Node, host, taskID string,
) (Output, error) {
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

	stream, err := rpc.PushData(runCtx)
	if err != nil {
		return nil, errors.Wrapf(err, "open stream to %s", host)
	}
	return &PushStream{
		stream:   stream,
		reqCache: &lrmrpb.PushDataRequest{},
	}, nil
}

func (p *PushStream) Write(rows []lrdd.Row) error {
	var err error
	for i, row := range rows {
		if len(p.reqCache.Data) == cap(p.reqCache.Data) {
			p.reqCache.Data = append(p.reqCache.Data, &lrdd.RawRow{})
		} else {
			p.reqCache.Data = p.reqCache.Data[:len(p.reqCache.Data)+1]
			if p.reqCache.Data[len(p.reqCache.Data)-1] == nil {
				p.reqCache.Data[len(p.reqCache.Data)-1] = &lrdd.RawRow{}
			}
		}

		p.reqCache.Data[i].Key = row.Key
		p.reqCache.Data[i].Value, err = row.Value.MarshalMsg(p.reqCache.Data[i].Value)
		if err != nil {
			return err
		}
	}

	err = p.stream.Send(p.reqCache)
	if err != nil {
		return err
	}

	p.reqCache.RemainCapicityReset()

	return nil
}

func (p *PushStream) Close() error {
	_, err := p.stream.CloseAndRecv()
	return err
}
