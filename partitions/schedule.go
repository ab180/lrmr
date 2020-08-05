package partitions

import (
	"sort"

	"github.com/airbloc/logger"
	"github.com/therne/lrmr/node"
	"github.com/thoas/go-funk"
)

var log = logger.New("partition")

type nodeWithStats struct {
	*node.Node
	currentTasks int
}

func newNodeWithStats(n *node.Node) nodeWithStats {
	return nodeWithStats{Node: n, currentTasks: 0}
}

// Schedule creates partition partition to the nodes by given options.
func Schedule(workers []*node.Node, plans []Plan, opt ...ScheduleOption) (pp []Partitions, aa []Assignments) {
	opts := buildScheduleOptions(opt)

	nn := funk.Map(workers, newNodeWithStats)
	if !opts.DisableShufflingNodes {
		nn = funk.Shuffle(nn)
	}
	nodes := nn.([]nodeWithStats)

	for i := range plans {
		plan := &plans[i]

		// select top N freest nodes
		sort.SliceStable(nodes, func(i, j int) bool {
			return nodes[i].currentTasks < nodes[j].currentTasks
		})
		lenCandidates := len(nodes)
		if plan.MaxNodes != Auto {
			lenCandidates = plan.MaxNodes
		}

		var candidates []nodeWithStats
		if len(plan.DesiredNodeAffinity) > 0 {
			slot := 0
			for i := 0; i < lenCandidates; i++ {
				n, nextSlot := selectNextNodeWithAffinity(nodes, opts.Master, plan.DesiredNodeAffinity, slot)
				if n != nil {
					candidates = append(candidates, *n)
				}
				slot = nextSlot
			}
			if len(candidates) == 0 {
				log.Warn("Warning: desired node affinity ({}) of plan #{} cannot be satisfied.", plan.DesiredNodeAffinity, i)
				candidates = nodes[:lenCandidates]
			}
		} else {
			candidates = nodes[:lenCandidates]
		}

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

		if plan.Partitioner == nil {
			// sets default partitioner: if adjacent partitions are equal,
			// it can be preserved. otherwise, it needs to be shuffled.
			if i > 0 && i < len(plans)-1 && plan.Equal(plans[i+1]) {
				plan.Partitioner = NewPreservePartitioner()
			} else {
				plan.Partitioner = NewShuffledPartitioner()
			}
		}
		var partitions []Partition
		if i == 0 {
			partitions = []Partition{{ID: InputPartitionID}}
		} else if IsPreserved(plans[i-1].Partitioner) && len(pp) > 0 {
			partitions = pp[i-1].Partitions
		} else {
			partitions = plans[i-1].Partitioner.PlanNext(numExecutors)
		}
		pp = append(pp, New(plan.Partitioner, partitions))

		if i > 0 {
			if IsPreserved(plans[i-1].Partitioner) && len(aa) > 0 {
				// ensure that adjacent preserved partitions have exact same assignments
				aa = append(aa, aa[i-1])
				continue
			}
		}

		curSlot := 0
		assignments := make([]Assignment, len(partitions))
		for j, p := range partitions {
			var selected *nodeWithStats
			if len(p.AssignmentAffinity) > 0 {
				selected, curSlot = selectNextNodeWithAffinity(candidates, opts.Master, p.AssignmentAffinity, curSlot)
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

func selectNextNode(nn []nodeWithStats, plan *Plan, curSlot int) (selected *nodeWithStats, nextSlot int) {
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

func selectNextNodeWithAffinity(nn []nodeWithStats, maybeMaster *node.Node, rules map[string]string, curSlot int) (selected *nodeWithStats, next int) {
	if expectedTyp, ok := rules["Type"]; ok && expectedTyp == string(node.Master) && maybeMaster != nil {
		// explicit selection of master node
		return &nodeWithStats{Node: maybeMaster, currentTasks: 0}, curSlot
	}
	for slot := curSlot; slot < curSlot+len(nn); slot++ {
		n := &nn[slot%len(nn)]
		if satisfiesAffinity(n.Node, rules) {
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

type ScheduleOptions struct {
	DisableShufflingNodes bool
	Master                *node.Node
}

type ScheduleOption func(o *ScheduleOptions)

func WithoutShufflingNodes() ScheduleOption {
	return func(o *ScheduleOptions) {
		o.DisableShufflingNodes = true
	}
}

func WithMaster(n *node.Node) ScheduleOption {
	if n.Type != node.Master {
		panic("given node " + n.Host + " is not a master")
	}
	return func(o *ScheduleOptions) {
		o.Master = n
	}
}

func buildScheduleOptions(opts []ScheduleOption) (options ScheduleOptions) {
	for _, optFn := range opts {
		optFn(&options)
	}
	return options
}
