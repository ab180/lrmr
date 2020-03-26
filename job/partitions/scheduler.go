package partitions

import (
	"context"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/node"
	"math"
	"sort"
	"strconv"
)

type Scheduler struct {
	stats []nodeWithStats
}

func NewSchedulerWithNodes(ctx context.Context, coord coordinator.Coordinator, nodes []*node.Node) (*Scheduler, error) {
	stats := make([]nodeWithStats, len(nodes))

	for i, n := range nodes {
		//cnt, err := coord.ReadCounter(ctx, path.Join(nodeStatusNs, n.ID, "totalTasks"))
		//if err != nil {
		//	return nil, errors.Wrapf(err, "failed to read total task of %s", n.Host)
		//}
		stats[i] = nodeWithStats{Node: n, currentTasks: 0}
	}
	return &Scheduler{
		stats: stats,
	}, nil
}

func (s *Scheduler) Plan(opts ...PlanOptions) (lp LogicalPlans, pp PhysicalPlans) {
	opt := buildPartitionOptions(opts)
	lp.IsElastic = opt.isElastic

	// select top N freest nodes
	sort.Slice(s.stats, func(i, j int) bool {
		return s.stats[i].currentTasks < s.stats[j].currentTasks
	})
	lenCandidates := min(opt.maxNodes, len(s.stats))
	candidates := make([]nodeWithStats, lenCandidates)
	copy(candidates[:], s.stats[:lenCandidates])

	var totalPartitions int
	if len(opt.fixedKeys) > 0 {
		totalPartitions = len(opt.fixedKeys)
	} else if opt.fixedCounts > 0 {
		totalPartitions = opt.fixedCounts
	} else {
		for _, n := range candidates {
			executors := n.Executors
			if opt.executorsPerNode != auto {
				executors = opt.executorsPerNode
			}
			totalPartitions += executors
		}
	}

	slot := 0
	for i := 0; i < totalPartitions; i++ {
		var selected *nodeWithStats
		for {
			selected = &candidates[slot%len(candidates)]
			maxCount := selected.Executors
			if opt.executorsPerNode != auto {
				maxCount = opt.executorsPerNode
			}
			if selected.currentTasks >= maxCount {
				// search another node
				slot += 1
				//log.Printf("Selected %s (%d running) exceeds maximum %d", selected.Host, selected.currentTasks, maxCount)
				continue
			}
			break
		}
		slot += 1
		selected.currentTasks += 1

		var pk string
		if len(opt.fixedKeys) > 0 {
			pk = opt.fixedKeys[i]
		} else {
			pk = strconv.Itoa(i)
		}
		lp.Keys = append(lp.Keys, pk)
		pp = append(pp, PhysicalPlan{Key: pk, Node: selected.Node})
	}
	return lp, pp
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
