package output

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
)

type Writer interface {
	Write(lrdd.Row) error
}

type StreamWriter struct {
	sh          *Shards
	partitioner Partitioner
}

func NewStreamWriter(sh *Shards) Writer {
	var hosts []string
	for _, shard := range sh.Desc.Shards {
		hosts = append(hosts, shard.Host)
	}

	var p Partitioner
	switch sh.Desc.Partitioner.Type {
	case lrmrpb.Partitioner_HASH_KEY:
		p = NewHashKeyPartitioner(sh.Desc.Partitioner.KeyColumn, hosts)
	case lrmrpb.Partitioner_FINITE_KEY:
		p = NewFiniteKeyPartitioner(sh.Desc.Partitioner.KeyColumn, sh.Desc.Partitioner.KeyToHost)
	default:
		p = NewFanoutPartitioner(hosts)
	}

	return &StreamWriter{
		sh:          sh,
		partitioner: p,
	}
}

func (s *StreamWriter) Write(d lrdd.Row) error {
	host, err := s.partitioner.DetermineHost(d)
	if err != nil {
		return err
	}
	return s.sh.Shards[host].Send(d)
}
