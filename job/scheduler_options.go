package job

import "math"

type schedulerOptions struct {
	maxNodes            int
	maxExecutorsPerNode int
	fixedPartitionKeys  []string
}

type SchedulerOptions func(*schedulerOptions)

func WithPartitionKeys(keys []string) SchedulerOptions {
	return func(o *schedulerOptions) {
		o.fixedPartitionKeys = keys
	}
}

func WithLimitNodes(n int) SchedulerOptions {
	return func(o *schedulerOptions) {
		o.maxNodes = n
	}
}

func WithLimitExecutorsPerNode(n int) SchedulerOptions {
	return func(o *schedulerOptions) {
		o.maxExecutorsPerNode = n
	}
}

func buildSchedulerOptions(opts []SchedulerOptions) *schedulerOptions {
	o := &schedulerOptions{
		maxNodes:            -1,
		maxExecutorsPerNode: math.MaxInt64,
	}
	for _, optApplyFn := range opts {
		optApplyFn(o)
	}
	return o
}
