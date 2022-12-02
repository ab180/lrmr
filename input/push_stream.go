package input

import (
	"context"
	"io"

	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/lrmrpb"
	"github.com/pkg/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type PushStream struct {
	stream lrmrpb.Node_PushDataServer
	reader *Reader
}

func NewPushStream(r *Reader, stream lrmrpb.Node_PushDataServer) *PushStream {
	return &PushStream{
		stream: stream,
		reader: r,
	}
}

func (p *PushStream) Dispatch() error {
	p.reader.Add()
	defer p.reader.Done()

	for {
		req, err := p.stream.Recv()
		if err != nil {
			if status.Code(err) == codes.Canceled || errors.Cause(err) == context.Canceled || err == io.EOF {
				return nil
			}
			return errors.Wrap(err, "stream dispatch")
		}

		rows := make([]*lrdd.Row, len(req.Data))
		for i, row := range req.Data {
			value := lrdd.GetValue(p.reader.RowID())
			_, err := value.UnmarshalMsg(row.Value)
			if err != nil {
				return err
			}

			rows[i] = &lrdd.Row{
				Key:   row.Key,
				Value: value,
			}
		}

		p.reader.Write(rows)
	}
}

func (p *PushStream) CloseWithStatus(st job.Status) error {
	return p.stream.SendMsg(st)
}
