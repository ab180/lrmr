package output

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/segmentio/fasthash/fnv1a"
	"github.com/therne/lrmr/lrdd"
)

var (
	ErrNoOutput = errors.New("no output")
)

type Partitioner interface {
	DetermineHost(row *lrdd.Row) (string, error)
}

type finiteKeyPartitioner struct {
	key       string
	keyToHost map[string]string
}

func NewFiniteKeyPartitioner(keyToHost map[string]string) Partitioner {
	return &finiteKeyPartitioner{keyToHost: keyToHost}
}

func (f *finiteKeyPartitioner) DetermineHost(row *lrdd.Row) (host string, err error) {
	host, ok := f.keyToHost[row.Key]
	if !ok {
		err = fmt.Errorf("unknown key: %s", row.Key)
		return
	}
	return
}

type hashKeyPartitioner struct {
	hosts []string
}

func NewHashKeyPartitioner(hosts []string) Partitioner {
	return &hashKeyPartitioner{hosts: hosts}
}

func (h *hashKeyPartitioner) DetermineHost(row *lrdd.Row) (string, error) {
	// uses Fowler–Noll–Vo hash to determine output shard
	slot := fnv1a.HashString64(row.Key) % uint64(len(h.hosts))
	return h.hosts[slot], nil
}

func NewFanoutPartitioner(hosts []string) Partitioner {
	return &fanoutPartitioner{hosts: hosts}
}

type fanoutPartitioner struct {
	hosts      []string
	sentEvents uint64
}

func (f *fanoutPartitioner) DetermineHost(*lrdd.Row) (string, error) {
	if len(f.hosts) == 0 {
		return "", ErrNoOutput
	}
	shard := f.sentEvents % uint64(len(f.hosts))
	f.sentEvents++
	return f.hosts[shard], nil
}
