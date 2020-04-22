package input

import (
	"context"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrmrpb"
	"io"
)

type PushStream struct {
	stream lrmrpb.Worker_PushDataServer
	reader *Reader
}

func NewPushStream(r *Reader, stream lrmrpb.Worker_PushDataServer) *PushStream {
	return &PushStream{
		stream: stream,
		reader: r,
	}
}

func (p *PushStream) Dispatch(ctx context.Context) error {
	p.reader.Add(p)
	defer p.reader.Done()

	errChan := make(chan error)
	go func() {
		for {
			req, err := p.stream.Recv()
			if err != nil {
				errChan <- err
				return
			}
			p.reader.C <- req.GetData()
		}
	}()

	select {
	case err := <-errChan:
		if err == io.EOF || err == context.Canceled {
			return nil
		}
		return errors.Wrap(err, "stream dispatch")
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *PushStream) CloseWithStatus(st job.Status) error {
	return p.stream.SendMsg(st)
}
