package output

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
)

type FiniteKeyPartitioner struct {
	keyColumn string
	keyToHost map[string]string

	connections     map[string]*connection
	outputBufferLen int

	log logger.Logger
}

func NewFiniteKeyPartitioner() Output {
	return &FiniteKeyPartitioner{
		connections: make(map[string]*connection),
		log:         logger.New("finite-partitioner"),

		// TODO: provide option
		outputBufferLen: 100,
	}
}

func (f *FiniteKeyPartitioner) Connect(ctx context.Context, self *node.Node, desc *lrmrpb.Output) error {
	f.keyColumn = desc.Partitioner.PartitionKeyColumn
	f.keyToHost = desc.Partitioner.PartitionKeyToHost

	for _, s := range desc.Shards {
		conn, err := newConnection(ctx, self, s.Host, s.TaskID, f.outputBufferLen)
		if err != nil {
			f.log.Error("unable to connect {}: {}", s.Host, err)
			continue
		}
		f.connections[s.Host] = conn
	}
	return nil
}

func (f *FiniteKeyPartitioner) Send(row lrdd.Row) error {
	key := row[f.keyColumn].(string)
	host, ok := f.keyToHost[key]
	if !ok {
		return fmt.Errorf("unknown key: %s", key)
	}
	return f.connections[host].send(row)
}

func (f *FiniteKeyPartitioner) Flush() error {
	for _, conn := range f.connections {
		if err := conn.flush(); err != nil {
			return err
		}
	}
	return nil
}

func (f *FiniteKeyPartitioner) Close() error {
	for _, conn := range f.connections {
		if err := conn.close(); err != nil {
			return err
		}
	}
	return nil
}
