package partitions

import (
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/node"
	"github.com/thoas/go-funk"
	"math"
	"sort"
)

var log = logger.New("partition")

type nodeWithStats struct {
	*node.Node
	currentTasks int
}

// Schedule creates partition partition to the nodes by given options.
func Schedule(workers []*node.Node, plans []Plan) (pp []Partitions, aa []Assignments) {
	nodes := funk.Map(workers, func(n *node.Node) nodeWithStats {
		return nodeWithStats{Node: n, currentTasks: 0}
	}).([]nodeWithStats)

	for i, plan := range plans {
		// select top N freest nodes
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].currentTasks < nodes[j].currentTasks
		})
		lenCandidates := len(nodes)
		if plan.MaxNodes != Auto {
			lenCandidates = plan.MaxNodes
		}
		candidates := make([]nodeWithStats, lenCandidates)
		copy(candidates[:], nodes[:lenCandidates])

		var numExecutors int
		if plan.DesiredCount == Auto {
			for _, n := range candidates {
				executors := n.Executors
				if plan.ExecutorsPerNode != Auto {
					executors = plan.ExecutorsPerNode
				}
				numExecutors += executors
			}
		} else {
			numExecutors = plan.DesiredCount
		}

		if plan.Partitioner == nil && i >= 1 {
			// sets default partitioner: if adjacent partitions are equal,
			// it can be preserved. otherwise, it needs to be shuffled.
			prevPlan := plans[i-1]
			if prevPlan.Equal(plan) {
				plan.Partitioner = NewPreservePartitioner()
			} else {
				plan.Partitioner = NewShuffledPartitioner()
			}
		}
		partitions := plan.Partitioner.Plan(numExecutors)
		pp = append(pp, New(plan.Partitioner, partitions))

		if _, isPreserved := plan.Partitioner.(*PreservePartitioner); isPreserved && aa != nil {
			aa = append(aa, aa[i-1])
			continue
		}

		curSlot := 0
		assignments := make([]Assignment, len(partitions))
		for j, p := range partitions {
			var selected *nodeWithStats
			if len(p.AssignmentAffinity) > 0 {
				selected, curSlot = selectNextNodeWithAffinity(candidates, p, curSlot)
				if selected == nil {
					log.Warn("Unable to find node satisfying affinity rule {} for partition {}.", p.AssignmentAffinity, p.ID)
					selected, curSlot = selectNextNode(candidates, plan, curSlot)
				}
			} else {
				selected, curSlot = selectNextNode(candidates, plan, curSlot)
			}
			selected.currentTasks += 1
			assignments[j] = Assignment{
				Partition: p,
				Node:      selected.Node,
			}
		}
		aa = append(aa, assignments)
	}
	return pp, aa
}

func selectNextNode(nn []nodeWithStats, plan Plan, curSlot int) (selected *nodeWithStats, nextSlot int) {
	for slot := curSlot; slot < curSlot+len(nn); slot++ {
		n := &nn[slot%len(nn)]
		maxCount := n.Executors
		if plan.ExecutorsPerNode != Auto {
			maxCount = plan.ExecutorsPerNode
		}
		if n.currentTasks < maxCount {
			return n, slot + 1
		}
		// search another node
	}
	// not found. ignore max task rule
	return &nn[curSlot%len(nn)], curSlot + 1
}

func selectNextNodeWithAffinity(nn []nodeWithStats, p Partition, curSlot int) (selected *nodeWithStats, next int) {
	for slot := curSlot; slot < curSlot+len(nn); slot++ {
		n := &nn[slot%len(nn)]
		if satisfiesAffinity(n.Node, p.AssignmentAffinity) {
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
