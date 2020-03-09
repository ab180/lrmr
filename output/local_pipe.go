package output

import (
	"context"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/therne/lrmr/lrmrpb"
	"google.golang.org/grpc/metadata"
	"io"
)

type LocalPipeStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	input  chan *lrmrpb.RunRequest
	output chan *empty.Empty
}

type LocalStreamClient interface {
	lrmrpb.Worker_RunTaskClient
	Close() error
}

func localPipe() (LocalStreamClient, lrmrpb.Worker_RunTaskServer) {
	ctx, cancel := context.WithCancel(context.Background())
	l := &LocalPipeStream{
		ctx:    ctx,
		cancel: cancel,
		input:  make(chan *lrmrpb.RunRequest, 1000000),
		output: make(chan *empty.Empty),
	}
	return l, l
}

func (l *LocalPipeStream) Send(req *lrmrpb.RunRequest) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// probably tried to send on closed channel
			err = io.EOF
		}
	}()
	l.input <- req
	return
}

func (l *LocalPipeStream) Recv() (*lrmrpb.RunRequest, error) {
	req, ok := <-l.input
	if !ok {
		return nil, io.EOF
	}
	return req, nil
}

// CloseSend closes an input stream on local client side and waits for the result.
func (l *LocalPipeStream) CloseAndRecv() (*empty.Empty, error) {
	_ = l.CloseSend()
	return <-l.output, nil
}

// CloseSend closes an input stream on local client side.
func (l *LocalPipeStream) CloseSend() error {
	if l.ctx.Err() != context.Canceled {
		close(l.input)
		l.cancel()
	}
	return nil
}

// SendAndClose closes an input stream on local server side.
func (l *LocalPipeStream) SendAndClose(msg *empty.Empty) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// probably tried to send on closed channel
			err = io.EOF
		}
	}()
	l.output <- msg
	close(l.output)
	return nil
}

func (l *LocalPipeStream) Context() context.Context {
	return l.ctx
}

func (l *LocalPipeStream) Header() (metadata.MD, error) {
	return metadata.Join(), nil
}

func (l *LocalPipeStream) Trailer() metadata.MD {
	return metadata.Join()
}

func (l *LocalPipeStream) SetHeader(metadata.MD) error {
	return nil
}

func (l *LocalPipeStream) SendHeader(metadata.MD) error {
	return nil
}

func (l *LocalPipeStream) SetTrailer(metadata.MD) {
	return
}

func (l *LocalPipeStream) SendMsg(m interface{}) error {
	panic("implement me")
}

func (l *LocalPipeStream) RecvMsg(m interface{}) error {
	panic("implement me")
}

func (l *LocalPipeStream) Close() error {
	return nil
}
