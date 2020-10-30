package partitions

type Context interface {
	PartitionID() string
}

type context struct {
	partitionID string
}

func NewContext(partitionID string) Context {
	return context{partitionID}
}

func (c context) PartitionID() string { return c.partitionID }
