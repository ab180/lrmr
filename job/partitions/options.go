package partitions

import "math"

const auto = -1

type partitionOptions struct {
	isElastic        bool
	maxNodes         int
	executorsPerNode int

	fixedCounts int
	fixedKeys   []string
}

type PlanOptions func(*partitionOptions)

func WithFixedKeys(keys []string) PlanOptions {
	return func(o *partitionOptions) {
		o.isElastic = false
		o.fixedKeys = keys
	}
}

func WithFixedCount(n int) PlanOptions {
	return func(o *partitionOptions) {
		o.fixedCounts = n
	}
}

func WithNoElastic() PlanOptions {
	return func(o *partitionOptions) {
		o.isElastic = false
	}
}

func WithLimitNodes(n int) PlanOptions {
	return func(o *partitionOptions) {
		o.maxNodes = n
	}
}

func WithExecutorsPerNode(n int) PlanOptions {
	return func(o *partitionOptions) {
		o.executorsPerNode = n
	}
}

func buildPartitionOptions(opts []PlanOptions) *partitionOptions {
	o := &partitionOptions{
		isElastic:        true,
		maxNodes:         math.MaxInt64,
		executorsPerNode: auto,
	}
	for _, optApplyFn := range opts {
		optApplyFn(o)
	}
	return o
}
