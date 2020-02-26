package output

import (
	"context"
	"fmt"
	"github.com/therne/lrmr/dataframe"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"google.golang.org/grpc"
)

type connection struct {
	taskID string
	self   *node.Node
	stream lrmrpb.Worker_RunTaskClient
	conn   *grpc.ClientConn
	buffer [][]byte
}

func newConnection(ctx context.Context, self *node.Node, host, taskID string, bufferLen int) (*connection, error) {
	conn, err := grpc.DialContext(ctx, host, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	client := lrmrpb.NewWorkerClient(conn)
	stream, err := client.RunTask(context.Background())
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

	return &connection{
		taskID: taskID,
		self:   self,
		stream: stream,
		conn:   conn,
		buffer: make([][]byte, 0, bufferLen),
	}, nil
}

func (c *connection) send(d dataframe.Row) error {
	c.buffer = append(c.buffer, d.Marshal())
	if len(c.buffer) == cap(c.buffer) {
		return c.flush()
	}
	return nil
}

func (c *connection) flush() (err error) {
	err = c.stream.Send(&lrmrpb.RunRequest{
		From: &lrmrpb.Node{
			Host: c.self.Host,
			ID:   c.self.ID,
		},
		TaskID: c.taskID,
		Inputs: c.buffer,
	})

	// this clears buffer length without GC
	c.buffer = c.buffer[:0]
	return
}

func (c *connection) close() error {
	if _, err := c.stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("stream result: %w", err)
	}
	return c.conn.Close()
}
