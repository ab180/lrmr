package output

import (
	"errors"
	"fmt"
	"github.com/segmentio/fasthash/fnv1a"
	"github.com/therne/lrmr/lrdd"
)

var (
	// ErrKeyNotFound is raised when given partition key column does not exist in data.
	ErrKeyNotFound = errors.New("key not found")
)

type Partitioner interface {
	DetermineHost(row lrdd.Row) (string, error)
}

type finiteKeyPartitioner struct {
	key       string
	keyToHost map[string]string
}

func NewFiniteKeyPartitioner(key string, keyToHost map[string]string) Partitioner {
	return &finiteKeyPartitioner{key: key, keyToHost: keyToHost}
}

func (f *finiteKeyPartitioner) DetermineHost(row lrdd.Row) (host string, err error) {
	key, ok := row[f.key].(string)
	if !ok {
		err = ErrKeyNotFound
		return
	}
	host, ok = f.keyToHost[key]
	if !ok {
		err = fmt.Errorf("unknown key: %s", key)
		return
	}
	return
}

type hashKeyPartitioner struct {
	key   string
	hosts []string
}

func NewHashKeyPartitioner(key string, hosts []string) Partitioner {
	return &hashKeyPartitioner{key: key, hosts: hosts}
}

func (h *hashKeyPartitioner) DetermineHost(row lrdd.Row) (string, error) {
	key, ok := row[h.key].(string)
	if !ok {
		return "", ErrKeyNotFound
	}
	// uses Fowler–Noll–Vo hash to determine output shard
	return h.hosts[fnv1a.HashString64(key)%uint64(len(h.hosts))], nil
}

func NewFanoutPartitioner(hosts []string) Partitioner {
	return &fanoutPartitioner{hosts: hosts}
}

type fanoutPartitioner struct {
	hosts      []string
	sentEvents uint64
}

func (f *fanoutPartitioner) DetermineHost(lrdd.Row) (string, error) {
	shard := f.sentEvents % uint64(len(f.hosts))
	f.sentEvents++
	return f.hosts[shard], nil
}
