package output

import (
	"context"
	"fmt"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"google.golang.org/grpc"
	"sync"
)

type Shard struct {
	taskID string
	self   *node.Node
	stream lrmrpb.Worker_RunTaskClient
	conn   *grpc.ClientConn

	buffer [][]byte
	lock   sync.Mutex
}

func DialShard(ctx context.Context, self *node.Node, host, taskID string, opt Options) (*Shard, error) {
	dialCtx, cancel := context.WithTimeout(ctx, opt.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, host, opt.DialOpts...)
	if err != nil {
		return nil, err
	}
	client := lrmrpb.NewWorkerClient(conn)
	stream, err := client.RunTask(context.Background(), grpc.MaxCallSendMsgSize(opt.MaxSendMsgSize))
	if err != nil {
		return nil, err
	}

	warmUpReq := &lrmrpb.RunRequest{
		TaskID: taskID,
		From: &lrmrpb.Node{
			Host: self.Host,
			ID:   self.ID,
		},
	}
	if err := stream.Send(warmUpReq); err != nil {
		return nil, fmt.Errorf("warm up: %w", err)
	}

	return &Shard{
		taskID: taskID,
		self:   self,
		stream: stream,
		conn:   conn,
		buffer: make([][]byte, 0, opt.BufferLength),
	}, nil
}

func (sw *Shard) Send(d lrdd.Row) error {
	sw.lock.Lock()
	sw.buffer = append(sw.buffer, d.Marshal())
	sw.lock.Unlock()

	if len(sw.buffer) == cap(sw.buffer) {
		return sw.Flush()
	}
	return nil
}

func (sw *Shard) Flush() (err error) {
	sw.lock.Lock()
	defer sw.lock.Unlock()

	err = sw.stream.Send(&lrmrpb.RunRequest{
		From: &lrmrpb.Node{
			Host: sw.self.Host,
			ID:   sw.self.ID,
		},
		TaskID: sw.taskID,
		Inputs: sw.buffer,
	})

	// this clears buffer length without GC
	sw.buffer = sw.buffer[:0]
	return
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
}

func DialShards(ctx context.Context, self *node.Node, desc *lrmrpb.Output, opt Options) (*Shards, error) {
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
	}, nil
}

func (sh *Shards) Flush() error {
	for _, shard := range sh.Shards {
		if err := shard.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (sh *Shards) Close() error {
	for _, shard := range sh.Shards {
		if err := shard.Close(); err != nil {
			return err
		}
	}
	return nil
}
