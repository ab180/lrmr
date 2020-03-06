package output

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
)

type Writer interface {
	Write(lrdd.Row) error
	Flush() error
}

type StreamWriter struct {
	sh          *Shards
	buffers     map[string][][]byte
	partitioner Partitioner
}

func NewStreamWriter(sh *Shards) Writer {
	buffers := make(map[string][][]byte)
	var hosts []string
	for _, shard := range sh.Desc.Shards {
		hosts = append(hosts, shard.Host)
		buffers[shard.Host] = make([][]byte, 0, sh.opt.BufferLength)
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
		buffers:     buffers,
		partitioner: p,
	}
}

func (s *StreamWriter) Write(d lrdd.Row) error {
	host, err := s.partitioner.DetermineHost(d)
	if err != nil {
		return err
	}
	s.buffers[host] = append(s.buffers[host], d.Marshal())
	if len(s.buffers[host]) == cap(s.buffers[host]) {
		return s.flushHost(host)
	}
	return nil
}

func (s *StreamWriter) flushHost(h string) (err error) {
	if len(s.buffers[h]) == 0 {
		return
	}
	err = s.sh.Shards[h].Send(s.buffers[h])
	s.buffers[h] = s.buffers[h][:0]
	return
}

func (s *StreamWriter) Flush() error {
	for host := range s.sh.Shards {
		if err := s.flushHost(host); err != nil {
			return err
		}
	}
	return nil
}
