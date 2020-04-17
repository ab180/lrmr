package partitions

import "math"

const auto = -1

type PlanOptions struct {
	planner LogicalPlanner

	isElastic        bool
	maxNodes         int
	executorsPerNode int

	fixedCounts int
	fixedKeys   []string
	noPartition bool
}

type PlanOption func(*PlanOptions)

type LogicalPlanner interface {
	Plan() LogicalPlans
}

func WithLogicalPlanner(p LogicalPlanner) PlanOption {
	return func(o *PlanOptions) {
		o.planner = p
	}
}

func WithEmpty() PlanOption {
	return func(o *PlanOptions) {
		o.noPartition = true
	}
}

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
