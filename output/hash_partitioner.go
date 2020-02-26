package output

import (
	"context"
	"github.com/airbloc/logger"
	"github.com/segmentio/fasthash/fnv1a"
	"github.com/therne/lrmr/dataframe"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
)

type HashPartitioner struct {
	keyColumn string
	hosts     []string

	connections     map[string]*connection
	outputBufferLen int

	log logger.Logger
}

func NewHashPartitioner() Output {
	return &HashPartitioner{
		connections: make(map[string]*connection),
		log:         logger.New("hash-partitioner"),

		// TODO: provide option
		outputBufferLen: 100,
	}
}

func (hp *HashPartitioner) Connect(ctx context.Context, self *node.Node, desc *lrmrpb.Output) error {
	hp.keyColumn = desc.Partitioner.PartitionKeyColumn

	for _, s := range desc.Shards {
		conn, err := newConnection(ctx, self, s.Host, s.TaskID, hp.outputBufferLen)
		if err != nil {
			hp.log.Error("unable to connect {}: {}", s.Host, err)
			continue
		}
		hp.connections[s.Host] = conn
		hp.hosts = append(hp.hosts, s.Host)
	}
	return nil
}

func (hp *HashPartitioner) Send(row dataframe.Row) error {
	key := row[hp.keyColumn].(string)

	// uses Fowler–Noll–Vo hash to determine output shard
	host := hp.hosts[fnv1a.HashString64(key)%uint64(len(hp.hosts))]
	return hp.connections[host].send(row)
}

func (hp *HashPartitioner) Flush() error {
	for _, conn := range hp.connections {
		if err := conn.flush(); err != nil {
			return err
		}
	}
	return nil
}

func (hp *HashPartitioner) Close() error {
	for _, conn := range hp.connections {
		if err := conn.close(); err != nil {
			return err
		}
	}
	return nil
}
