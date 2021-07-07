package input

import (
	"context"
	"io"

	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrmrpb"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/errorist"
)

type PushStream struct {
	stream lrmrpb.Node_PushDataServer
	reader *Reader
	log    logger.Logger
}

func NewPushStream(r *Reader, stream lrmrpb.Node_PushDataServer, logTag string) *PushStream {
	return &PushStream{
		stream: stream,
		reader: r,
		log:    logger.New("lrmr").WithAttrs(logger.Attrs{"TaskID": logTag}),
	}
}

func (p *PushStream) Dispatch(ctx context.Context) error {
	p.reader.Add(p)
	defer p.reader.Done()

	errChan := make(chan error, 1)
	go func() {
		defer errorist.RecoverWithErrChan(errChan)
		for {
			req, err := p.stream.Recv()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				errChan <- err
				return
			}
			select {
			case p.reader.C <- req.Data:
			default:
				p.log.Debug("Input backpressure on")
				p.reader.C <- req.Data
				p.log.Debug("Backpressure released on")
			}
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
