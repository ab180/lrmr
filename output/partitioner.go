package output

import (
	"github.com/pkg/errors"
	"github.com/segmentio/fasthash/fnv1a"
	"github.com/therne/lrmr/lrdd"
	"strconv"
)

var (
	ErrNoOutput = errors.New("no output")
)

type Partitioner interface {
	DeterminePartitionKey(row *lrdd.Row) (key string, err error)
}

type finiteKeyPartitioner struct{}

func NewFiniteKeyPartitioner() Partitioner {
	return &finiteKeyPartitioner{}
}

func (f *finiteKeyPartitioner) DeterminePartitionKey(row *lrdd.Row) (string, error) {
	return row.Key, nil
}

type hashKeyPartitioner struct {
	numPartitions uint64
}

func NewHashKeyPartitioner(numPartitions int) Partitioner {
	return &hashKeyPartitioner{numPartitions: uint64(numPartitions)}
}

func (h *hashKeyPartitioner) DeterminePartitionKey(row *lrdd.Row) (string, error) {
	// uses Fowler–Noll–Vo hash to determine output shard
	slot := fnv1a.HashString64(row.Key) % h.numPartitions
	return strconv.FormatUint(slot, 10), nil
}

func NewShuffledPartitioner(numPartitions int) Partitioner {
	return &shuffledPartitioner{numPartitions: numPartitions}
}

type shuffledPartitioner struct {
	numPartitions int
	sentEvents    int
}

func (f *shuffledPartitioner) DeterminePartitionKey(*lrdd.Row) (string, error) {
	if f.numPartitions == 0 {
		return "", ErrNoOutput
	}
	slot := f.sentEvents % f.numPartitions
	f.sentEvents++
	return strconv.Itoa(slot), nil
}
