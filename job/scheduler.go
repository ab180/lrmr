package job

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/node"
	"math"
	"path"
	"sort"
	"strconv"
)

type ScheduleResult struct {
	PartitionKey string
	Node         *node.Node
}

type currentTasksOfNode struct {
	node         *node.Node
	currentTasks int64
}

// Scheduler schedules jobs to nodes in the cluster.
type Scheduler interface {
	// ScheduleWorks assigns given workloads to given candidate nodes
	// by balancing them with each work's complexity.
	// It will return a list of workload reference ID, which is used for releasing the workload.
	ScheduleTasks(ctx context.Context, opts ...SchedulerOptions) ([]ScheduleResult, error)
}

func (m *jobManager) ScheduleTasks(ctx context.Context, opts ...SchedulerOptions) ([]ScheduleResult, error) {
	opt := buildSchedulerOptions(opts)

	allNodes, err := m.nodeManager.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes information: %v", err)
	}
	if len(allNodes) == 0 {
		return nil, node.ErrNodeNotFound
	}

	var candidates []*node.Node
	if opt.maxNodes == -1 {
		candidates = allNodes
	} else {
		// select top N freest nodes
		cc := make([]currentTasksOfNode, len(allNodes))
		for _, n := range allNodes {
			cnt, err := m.crd.ReadCounter(ctx, path.Join(nodeStatusNs, n.ID, "totalTasks"))
			if err != nil {
				return nil, errors.Wrapf(err, "failed to read total task of %s", n.Host)
			}
			cc = append(cc, currentTasksOfNode{node: n, currentTasks: cnt})
		}
		sort.Slice(cc, func(i, j int) bool {
			return cc[i].currentTasks < cc[j].currentTasks
		})
		candidates = make([]*node.Node, opt.maxNodes)
		for i := 0; i < opt.maxNodes; i++ {
			candidates[i] = cc[i].node
		}
	}

	var totalPartitions int
	if len(opt.fixedPartitionKeys) > 0 {
		totalPartitions = len(opt.fixedPartitionKeys)
	} else {
		for _, n := range candidates {
			totalPartitions += min(n.Executors, opt.maxExecutorsPerNode)
		}
	}

	slot := 0
	scheduledCounts := make(map[string]int)
	results := make([]ScheduleResult, totalPartitions)
	for i := 0; i < totalPartitions; i++ {
		var selected *node.Node
		for {
			selected = candidates[slot%len(candidates)]
			maxCount := min(selected.Executors, opt.maxExecutorsPerNode)
			if scheduledCounts[selected.ID] < maxCount {
				break
			}
			slot += 1
		}
		slot += 1
		scheduledCounts[selected.ID] += 1

		var pk string
		if len(opt.fixedPartitionKeys) > 0 {
			pk = opt.fixedPartitionKeys[i]
		} else {
			pk = strconv.Itoa(i)
		}
		results[i] = ScheduleResult{PartitionKey: pk, Node: selected}
	}
	return results, nil
}

func min(nn ...int) int {
	min := math.MaxInt64
	for _, n := range nn {
		if n < min {
			min = n
		}
	}
	return min
}
