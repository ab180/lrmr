package output

import (
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"time"
)

var log = logger.New("output")

type Writer interface {
	Write(*lrdd.Row) error
	Errors() <-chan error
	FlushAndClose() error
}

type StreamWriter struct {
	sh          *Shards
	buffers     map[string][]*lrdd.Row
	partitioner Partitioner

	dataChan  chan *lrdd.Row
	errorChan chan error
}

func NewStreamWriter(sh *Shards) Writer {
	buffers := make(map[string][]*lrdd.Row)
	var hosts []string
	for host := range sh.Shards {
		hosts = append(hosts, host)
		buffers[host] = make([]*lrdd.Row, 0, sh.opt.BufferLength)
	}

	var p Partitioner
	switch sh.Desc.Partitioner.Type {
	case lrmrpb.Partitioner_HASH_KEY:
		p = NewHashKeyPartitioner(hosts)
	case lrmrpb.Partitioner_FINITE_KEY:
		p = NewFiniteKeyPartitioner(sh.Desc.Partitioner.KeyToHost)
	default:
		p = NewFanoutPartitioner(hosts)
	}

	s := &StreamWriter{
		sh:          sh,
		buffers:     buffers,
		partitioner: p,
		dataChan:    make(chan *lrdd.Row, 1000000),
		errorChan:   make(chan error),
	}
	go s.dispatch()
	return s
}

func (s *StreamWriter) dispatch() {
	for d := range s.dataChan {
		host, err := s.partitioner.DetermineHost(d)
		if err != nil {
			if err == ErrNoOutput {
				return
			}
			s.errorChan <- err
			return
		}
		s.buffers[host] = append(s.buffers[host], d)
		if len(s.buffers[host]) == cap(s.buffers[host]) {
			err := s.flushHost(host)
			if err != nil {
				s.errorChan <- err
			}
		}
	}
}

func (s *StreamWriter) Write(d *lrdd.Row) error {
	s.dataChan <- d
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

func (s *StreamWriter) Errors() <-chan error {
	return s.errorChan
}

func (s *StreamWriter) FlushAndClose() error {
	for len(s.dataChan) > 0 {
		// wait for the queue to drain
		time.Sleep(100 * time.Millisecond)
	}
	close(s.dataChan)
	close(s.errorChan)

	for host := range s.sh.Shards {
		if err := s.flushHost(host); err != nil {
			return errors.Wrapf(err, "flush %s", host)
		}
	}
	return nil
}
