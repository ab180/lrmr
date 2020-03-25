package input

import (
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

func (p *PushStream) Dispatch() error {
	p.reader.Add(p)
	for {
		req, err := p.stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrap(err, "stream dispatch")
		}
		p.reader.C <- req.GetData()
	}
	p.reader.Done()
	return nil
}

func (p *PushStream) CloseWithStatus(st job.Status) error {
	return p.stream.SendMsg(st)
}
