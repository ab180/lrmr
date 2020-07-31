package partitions

import (
	"errors"
	"github.com/segmentio/fasthash/fnv1a"
	"github.com/therne/lrmr/internal/serialization"
	"github.com/therne/lrmr/lrdd"
	"go.uber.org/atomic"

	"strconv"
)

// ErrNoOutput is returned by Partitioner.DeterminePartition when there's no
// corresponding partition found with the key of given row.
var ErrNoOutput = errors.New("no output")

type Context interface {
	PartitionID() string
}

type Partitioner interface {
	PlanNext(numExecutors int) []Partition
	DeterminePartition(c Context, r *lrdd.Row, numOutputs int) (id string, err error)
}

type SerializablePartitioner struct {
	Partitioner
}

func WrapPartitioner(p Partitioner) SerializablePartitioner {
	return SerializablePartitioner{p}
}

func UnwrapPartitioner(p Partitioner) Partitioner {
	if sp, ok := p.(SerializablePartitioner); ok {
		return sp.Partitioner
	}
	return p
}

func (s SerializablePartitioner) MarshalJSON() ([]byte, error) {
	return serialization.SerializeStruct(s.Partitioner)
}

func (s *SerializablePartitioner) UnmarshalJSON(data []byte) error {
	v, err := serialization.DeserializeStruct(data)
	if err != nil {
		return err
	}
	s.Partitioner = v.(Partitioner)
	return nil
}

// PlanForNumberOf creates partition for the number of executors.
// It uses its index number for each partition's ID.
func PlanForNumberOf(numExecutors int) []Partition {
	pp := make([]Partition, numExecutors)
	for i := 0; i < numExecutors; i++ {
		pp[i] = Partition{
			ID:        strconv.Itoa(i),
			IsElastic: true,
		}
	}
	return pp
}

// FiniteKeyPartitioner evenly distributes a predefined set of keys to the nodes.
type FiniteKeyPartitioner struct {
	KeySet map[string]struct{}
}

func NewFiniteKeyPartitioner(keys []string) Partitioner {
	keySet := make(map[string]struct{})
	for _, k := range keys {
		keySet[k] = struct{}{}
	}
	return &FiniteKeyPartitioner{keySet}
}

// PlanNext creates partitions for the number of keys. Uses row key as partition ID.
func (f *FiniteKeyPartitioner) PlanNext(int) (partitions []Partition) {
	for key := range f.KeySet {
		partitions = append(partitions, Partition{
			ID:        key,
			IsElastic: false,
		})
	}
	return partitions
}

func (f *FiniteKeyPartitioner) DeterminePartition(c Context, r *lrdd.Row, numOutputs int) (id string, err error) {
	if _, ok := f.KeySet[r.Key]; !ok {
		err = ErrNoOutput
		return
	}
	return r.Key, nil
}

type hashKeyPartitioner struct{}

func NewHashKeyPartitioner() Partitioner {
	return &hashKeyPartitioner{}
}

func (h *hashKeyPartitioner) PlanNext(numExecutors int) []Partition {
	return PlanForNumberOf(numExecutors)
}

func (h *hashKeyPartitioner) DeterminePartition(c Context, r *lrdd.Row, numOutputs int) (id string, err error) {
	// uses Fowler–Noll–Vo hash to determine output shard
	slot := fnv1a.HashString64(r.Key) % uint64(numOutputs)
	return strconv.FormatUint(slot, 10), nil
}

type ShuffledPartitioner struct {
	sentEvents *atomic.Uint64
}

func NewShuffledPartitioner() Partitioner {
	return &ShuffledPartitioner{}
}

func (f *ShuffledPartitioner) PlanNext(numExecutors int) []Partition {
	return PlanForNumberOf(numExecutors)
}

func (f *ShuffledPartitioner) DeterminePartition(c Context, r *lrdd.Row, numOutputs int) (id string, err error) {
	slot := int(f.sentEvents.Add(1)) % numOutputs
	return strconv.Itoa(slot), nil
}

type PreservePartitioner struct{}

func NewPreservePartitioner() Partitioner {
	return &PreservePartitioner{}
}

func (p PreservePartitioner) PlanNext(numExecutors int) []Partition {
	return PlanForNumberOf(numExecutors)
}

func (p PreservePartitioner) DeterminePartition(c Context, _ *lrdd.Row, _ int) (id string, err error) {
	return c.PartitionID(), nil
}

func isPreserved(p Partitioner) bool {
	_, ok := p.(*PreservePartitioner)
	return ok
}

type masterAssigner struct {
	Partitioner SerializablePartitioner
}

// WithAssignmentToMaster wraps existing partitioner to assign partition to master nodes.
func WithAssignmentToMaster(p Partitioner) Partitioner {
	return &masterAssigner{Partitioner: WrapPartitioner(p)}
}

// PlanNext overrides wrapped plans from partitioner with adding affinity to master nodes.
func (m masterAssigner) PlanNext(numExecutors int) []Partition {
	planned := m.Partitioner.PlanNext(numExecutors)
	for i := range planned {
		if len(planned[i].AssignmentAffinity) == 0 {
			planned[i].AssignmentAffinity = make(map[string]string)
		}
		planned[i].AssignmentAffinity["Type"] = "master"
	}
	return planned
}

func (m masterAssigner) DeterminePartition(c Context, r *lrdd.Row, numOutputs int) (id string, err error) {
	return m.Partitioner.DeterminePartition(c, r, numOutputs)
}
