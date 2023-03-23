package partitions

const (
	InputPartitionID   = "_input"
	CollectPartitionID = "_collect"
)

// Partitions represents partitions in a stage.
type Partitions struct {
	Partitioner SerializablePartitioner `json:"partitioner"`
	Partitions  []Partition             `json:"partitions"`
}

func New(p Partitioner, partitions []Partition) Partitions {
	return Partitions{
		Partitioner: SerializablePartitioner{p},
		Partitions:  partitions,
	}
}

type Partition struct {
	ID string

	// IsElastic indicates that this partition allows work stealing from other executors.
	IsElastic bool

	// AssignmentAffinity is a set of equality rules to place partition into physical nodes.
	AssignmentAffinity map[string]string

	// Cost is the cost of this partition. It is used to determine the node.
	Cost uint64
}
