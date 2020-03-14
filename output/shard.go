package output

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"google.golang.org/grpc"
	"io"
)

type Shard struct {
	taskID string
	self   *lrmrpb.Node
	stream lrmrpb.Worker_RunTaskClient
	conn   io.Closer
}

type Node interface {
	NodeInfo() *lrmrpb.Node
}

func DialShard(ctx context.Context, self Node, host, taskID string, opt Options) (*Shard, error) {
	conn, stream, err := connect(ctx, self, host, opt)
	if err != nil {
		return nil, errors.Wrapf(err, "connect to %s", host)
	}
	warmUpReq := &lrmrpb.RunRequest{
		TaskID: taskID,
		From:   self.NodeInfo(),
	}
	if err := stream.Send(warmUpReq); err != nil {
		return nil, errors.Wrap(err, "warm up")
	}
	return &Shard{
		taskID: taskID,
		self:   self.NodeInfo(),
		stream: stream,
		conn:   conn,
	}, nil
}

func connect(ctx context.Context, self Node, host string, opt Options) (io.Closer, lrmrpb.Worker_RunTaskClient, error) {
	if self.NodeInfo().Host == host {
		//w, r := localPipe()
		//if workSrv, ok := self.(lrmrpb.WorkerServer); ok {
		//	go workSrv.RunTask(r)
		//}
		//return w, w, nil
	}
	dialCtx, cancel := context.WithTimeout(ctx, opt.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, host, opt.DialOpts...)
	if err != nil {
		return nil, nil, err
	}
	client := lrmrpb.NewWorkerClient(conn)
	stream, err := client.RunTask(context.Background(), grpc.MaxCallSendMsgSize(opt.MaxSendMsgSize))
	if err != nil {
		return nil, nil, err
	}
	return conn, stream, nil
}

func (sw *Shard) Send(inputs []*lrdd.Row) error {
	return sw.stream.Send(&lrmrpb.RunRequest{
		From:   sw.self,
		TaskID: sw.taskID,
		Inputs: inputs,
	})
}

func (sw *Shard) Close() error {
	if _, err := sw.stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("stream result: %w", err)
	}
	return sw.conn.Close()
}

type Shards struct {
	Shards map[string]*Shard
	Desc   *lrmrpb.Output
	opt    Options
}

func DialShards(ctx context.Context, self Node, desc *lrmrpb.Output, opt Options) (*Shards, error) {
	shards := make(map[string]*Shard)
	for _, s := range desc.Shards {
		sh, err := DialShard(ctx, self, s.Host, s.TaskID, opt)
		if err != nil {
			return nil, fmt.Errorf("connect %s: %w", s.Host, err)
		}
		shards[s.Host] = sh
	}
	return &Shards{
		Shards: shards,
		Desc:   desc,
		opt:    opt,
	}, nil
}

func (sh *Shards) Close() error {
	for _, shard := range sh.Shards {
		if err := shard.Close(); err != nil {
			return err
		}
	}
	return nil
}
