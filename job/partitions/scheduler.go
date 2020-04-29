package partitions

import (
	"context"
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/node"
	"github.com/thoas/go-funk"
	"math"
	"sort"
	"strconv"
)

type Scheduler struct {
	stats []nodeWithStats
	log   logger.Logger
}

func NewSchedulerWithNodes(ctx context.Context, coord coordinator.Coordinator, nodes []*node.Node) (*Scheduler, error) {
	stats := make([]nodeWithStats, len(nodes))

	for i, n := range nodes {
		// cnt, err := coord.ReadCounter(ctx, path.Join(nodeStatusNs, n.ID, "totalTasks"))
		// if err != nil {
		//	return nil, errors.Wrapf(err, "failed to read total task of %s", n.Host)
		// }
		stats[i] = nodeWithStats{Node: n, currentTasks: 0}
	}
	return &Scheduler{
		stats: stats,
		log:   logger.New("partitions-scheduler"),
	}, nil
}

// Plan assigns partition to the nodes by given options.
func (s *Scheduler) Plan(opts ...PlanOption) (lp LogicalPlans, pp PhysicalPlans) {
	opt := BuildPlanOptions(opts)
	if opt.noPartition {
		return
	}

	nodes := s.stats
	if len(opt.fixedNodes) > 0 {
		nodes = funk.Map(opt.fixedNodes, func(n *node.Node) nodeWithStats {
			return nodeWithStats{Node: n, currentTasks: 0}
		}).([]nodeWithStats)
	}

	// select top N freest nodes
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].currentTasks < nodes[j].currentTasks
	})
	lenCandidates := min(opt.maxNodes, len(nodes))
	candidates := make([]nodeWithStats, lenCandidates)
	copy(candidates[:], nodes[:lenCandidates])

	if opt.planner != nil {
		lp = opt.planner.Plan()
	} else {
		// create logical plan automatically
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
		for i := 0; i < totalPartitions; i++ {
			var pk string
			if len(opt.fixedKeys) > 0 {
				pk = opt.fixedKeys[i]
			} else {
				pk = strconv.Itoa(i)
			}
			lp = append(lp, LogicalPlan{
				Key:       pk,
				IsElastic: opt.isElastic,
			})
		}
	}

	curSlot := 0
	for _, l := range lp {
		var selected *nodeWithStats
		if len(l.NodeAffinityRules) > 0 {
			selected, curSlot = selectNextNodeWithAffinity(candidates, l, curSlot)
			if selected == nil {
				s.log.Warn("Unable to find node satisfying affinity rule {} for partition {}.", l.NodeAffinityRules, l.Key)
				selected, curSlot = selectNextNode(candidates, opt, curSlot)
			}
		} else {
			selected, curSlot = selectNextNode(candidates, opt, curSlot)
		}
		selected.currentTasks += 1
		pp = append(pp, PhysicalPlan{
			Key:  l.Key,
			Node: selected.Node,
		})
	}
	return lp, pp
}

func selectNextNode(nn []nodeWithStats, opt *PlanOptions, curSlot int) (selected *nodeWithStats, nextSlot int) {
	for slot := curSlot; slot < curSlot+len(nn); slot++ {
		n := &nn[slot%len(nn)]
		maxCount := n.Executors
		if opt.executorsPerNode != auto {
			maxCount = opt.executorsPerNode
		}
		if n.currentTasks < maxCount {
			return n, slot + 1
		}
		// search another node
	}
	// not found. ignore max task rule
	return &nn[curSlot%len(nn)], curSlot + 1
}

func selectNextNodeWithAffinity(nn []nodeWithStats, l LogicalPlan, curSlot int) (selected *nodeWithStats, next int) {
	for slot := curSlot; slot < curSlot+len(nn); slot++ {
		n := &nn[slot%len(nn)]
		if satisfiesAffinity(n.Node, l.NodeAffinityRules) {
			return n, slot + 1
		}
	}
	// not found. probably there's no node satisfying given affinity rules
	return nil, curSlot
}

func satisfiesAffinity(n *node.Node, rules map[string]string) bool {
	for k, v := range rules {
		if k == "Host" && v == n.Host {
			return true
		}
		if k == "ID" && v == n.ID {
			return true
		}
		for nk, nv := range n.Tag {
			if k == nk && v == nv {
				return true
			}
		}
	}
	return false
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
