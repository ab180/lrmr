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
		req := lrmrpb.GetPushDataRequest(0)
		err := p.stream.RecvMsg(req)
		if err != nil {
			if status.Code(err) == codes.Canceled || errors.Cause(err) == context.Canceled || err == io.EOF {
				return nil
			}
			return errors.Wrap(err, "stream dispatch")
		}

		// The user should manually return the []lrdd.Row to the pool.
		rows := lrdd.GetRows(len(req.Data))
		for i, row := range req.Data {
			value := lrdd.GetValue(p.reader.RowType())
			_, err := value.UnmarshalMsg(row.Value)
			if err != nil {
				return err
			}

			(*rows)[i].Key = row.Key
			(*rows)[i].Value = value
		}

		//p.reader.Write(rows)
		p.reader.Write(*rows)

		lrmrpb.PutPushDataRequest(req)
	}
}

func (p *PushStream) CloseWithStatus(st job.Status) error {
	return p.stream.SendMsg(st)
}
