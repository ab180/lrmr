package input

import (
	"context"
	"io"

	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrmrpb"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
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

func (p *PushStream) Dispatch(ctx context.Context) error {
	p.reader.Add(p)
	defer p.reader.Done()

	errChan := make(chan error, 1)
	go func() {
		defer func() {
			if err := logger.WrapRecover(recover()); err != nil {
				errChan <- err
			}
		}()
		for {
			req, err := p.stream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				errChan <- err
				return
			}
			p.reader.C <- req.Data
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
