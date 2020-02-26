package output

import (
	"context"
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"sync/atomic"
)

type RoundRobin struct {
	hosts []string
	count uint64

	connections     map[string]*connection
	outputBufferLen int

	log logger.Logger
}

func NewRoundRobin() Output {
	return &RoundRobin{
		connections: make(map[string]*connection),
		log:         logger.New("round-robin"),

		// TODO: provide option
		outputBufferLen: 100,
	}
}

func (r *RoundRobin) Connect(ctx context.Context, self *node.Node, desc *lrmrpb.Output) error {
	for _, s := range desc.Shards {
		conn, err := newConnection(ctx, self, s.Host, s.TaskID, r.outputBufferLen)
		if err != nil {
			r.log.Error("unable to connect {}: {}", s.Host, err)
			continue
		}
		r.connections[s.Host] = conn
		r.hosts = append(r.hosts, s.Host)
	}
	return nil
}

func (r *RoundRobin) Send(row lrdd.Row) error {
	index := (atomic.AddUint64(&r.count, 1) - 1) % uint64(len(r.hosts))
	return r.connections[r.hosts[index]].send(row)
}

func (r *RoundRobin) Flush() error {
	for _, conn := range r.connections {
		if err := conn.flush(); err != nil {
			return err
		}
	}
	return nil
}

func (r *RoundRobin) Close() error {
	for _, conn := range r.connections {
		if err := conn.close(); err != nil {
			return err
		}
	}
	return nil
}
