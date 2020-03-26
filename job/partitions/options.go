package partitions

import "math"

const auto = -1

type PlanOptions struct {
	isElastic        bool
	maxNodes         int
	executorsPerNode int

	fixedCounts int
	fixedKeys   []string
}

type PlanOption func(*PlanOptions)

func WithFixedKeys(keys []string) PlanOption {
	return func(o *PlanOptions) {
		o.isElastic = false
		o.fixedKeys = keys
	}
}

func WithFixedCount(n int) PlanOption {
	return func(o *PlanOptions) {
		o.fixedCounts = n
	}
}

func WithNoElastic() PlanOption {
	return func(o *PlanOptions) {
		o.isElastic = false
	}
}

func WithLimitNodes(n int) PlanOption {
	return func(o *PlanOptions) {
		o.maxNodes = n
	}
}

func WithExecutorsPerNode(n int) PlanOption {
	return func(o *PlanOptions) {
		o.executorsPerNode = n
	}
}

func BuildPlanOptions(opts []PlanOption) *PlanOptions {
	o := &PlanOptions{
		isElastic:        true,
		maxNodes:         math.MaxInt64,
		executorsPerNode: auto,
	}
	for _, optApplyFn := range opts {
		optApplyFn(o)
	}
	return o
}
