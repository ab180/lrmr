package partitions

import (
	"math/rand"
	"sort"

	"github.com/ab180/lrmr/cluster/node"
	"github.com/rs/zerolog/log"
	"github.com/samber/lo"
)

type nodeWithStatsSlice []*nodeWithStats

func newNodeWithStatsSlice(nodes []*node.Node) nodeWithStatsSlice {
	ns := make(nodeWithStatsSlice, len(nodes))
	for i, n := range nodes {
		ns[i] = newNodeWithStats(n)
	}

	return ns
}

func (ns *nodeWithStatsSlice) Shuffle() {
	length := len(*ns)

	result := make(nodeWithStatsSlice, length)

	for i, v := range rand.Perm(length) {
		result[i] = (*ns)[v]
	}

	*ns = result
}

func (ns *nodeWithStatsSlice) sortForFreestNodes() {
	sort.SliceStable(*ns, func(i, j int) bool {
		if (*ns)[i].currentCost == (*ns)[j].currentCost {
			return (*ns)[i].Host < (*ns)[j].Host
		}

		return (*ns)[i].currentCost < (*ns)[j].currentCost
	})
}

type nodeWithStats struct {
	*node.Node
	currentCost uint64
}

func newNodeWithStats(n *node.Node) *nodeWithStats {
	return &nodeWithStats{Node: n}
}

// Schedule creates partition partition to the nodes by given options.
func Schedule(workers []*node.Node, plans []Plan, opt ...ScheduleOption) (pp []Partitions, aa []Assignments) {
	opts := buildScheduleOptions(opt)

	nodes := newNodeWithStatsSlice(workers)
	if !opts.DisableShufflingNodes {
		nodes.Shuffle()
	}

	for i := range plans {
		plan := &plans[i]

		nodes.sortForFreestNodes()
		lenCandidates := len(nodes)
		if plan.MaxNodes != Auto {
			lenCandidates = plan.MaxNodes
		}

		var candidates nodeWithStatsSlice
		if len(plan.DesiredNodeAffinity) > 0 {
			for i := 0; i < lenCandidates; i++ {
				n := selectNextNodeWithAffinity(nodes, plan.DesiredNodeAffinity)
				if n != nil {
					candidates = append(candidates, n)
				}
			}
			if len(candidates) == 0 {
				log.Warn().
					Interface("desiredNodeAffinity", plan.DesiredNodeAffinity).
					Int("index", i).
					Msg("desired node affinity of plan cannot be satisfied")
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
			if i > 0 && (i == len(plans)-1 || plan.Equal(plans[i+1])) {
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
		for i := range partitions {
			// Set default cost for fallback.
			if partitions[i].Cost == 0 {
				partitions[i].Cost = 1
			}
		}
		pp = append(pp, New(plan.Partitioner, partitions))

		if i > 0 {
			if IsPreserved(plans[i-1].Partitioner) && len(aa) > 0 {
				// ensure that adjacent preserved partitions have exact same assignments
				aa = append(aa, aa[i-1])
				continue
			}
		}

		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i].Cost > partitions[j].Cost
		})
		assignments := make([]Assignment, len(partitions))
		for j, p := range partitions {
			var selected *nodeWithStats
			if len(p.AssignmentAffinity) > 0 {
				selected = selectNextNodeWithAffinity(candidates, p.AssignmentAffinity)
				if selected == nil {
					log.Warn().
						Interface("assignmentAffinity", p.AssignmentAffinity).
						Str("id", p.ID).
						Msg("unable to find node satisfying affinity")
					selected = selectNextNode(candidates, plan)
				}
			} else {
				selected = selectNextNode(candidates, plan)
			}
			selected.currentCost += p.Cost
			assignments[j] = Assignment{
				PartitionID: p.ID,
				Host:        selected.Node.Host,
			}
		}
		aa = append(aa, assignments)
	}
	return pp, aa
}

func selectNextNode(nn nodeWithStatsSlice, plan *Plan) (selected *nodeWithStats) {
	nn.sortForFreestNodes()

	return nn[0] // It's safe. nn is not empty by Pipeline.createJob function.
}

func selectNextNodeWithAffinity(nn nodeWithStatsSlice, rules map[string]string,
) (selected *nodeWithStats) {
	filteredNn := nodeWithStatsSlice(lo.Filter(nn, func(n *nodeWithStats, _ int) bool {
		return satisfiesAffinity(n.Node, rules)
	}))

	filteredNn.sortForFreestNodes()

	if len(filteredNn) == 0 {
		// not found. probably there's no node satisfying given affinity rules
		return nil
	}

	return filteredNn[0]
}

func satisfiesAffinity(n *node.Node, rules map[string]string) bool {
	for k, v := range rules {
		if k == "Host" && v == n.Host {
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
}

type ScheduleOption func(o *ScheduleOptions)

func WithoutShufflingNodes() ScheduleOption {
	return func(o *ScheduleOptions) {
		o.DisableShufflingNodes = true
	}
}

func buildScheduleOptions(opts []ScheduleOption) (options ScheduleOptions) {
	for _, optFn := range opts {
		optFn(&options)
	}
	return options
}
