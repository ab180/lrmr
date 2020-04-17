package partitions

import (
	"fmt"
	"github.com/therne/lrmr/node"
	"strings"
)

const nodeStatusNs = "status/nodes"

type nodeWithStats struct {
	*node.Node
	currentTasks int
}

type LogicalPlans []LogicalPlan

// AutoPlan indicates that scheduler needs to automatically create
// partitions with available nodes and executors.
var AutoPlan LogicalPlans = nil

type LogicalPlan struct {
	Key       string
	IsElastic bool

	NodeAffinityRules map[string]string
}

func (lp LogicalPlans) Keys() (kk []string) {
	for _, l := range lp {
		kk = append(kk, l.Key)
	}
	return
}

// PhysicalPlan is an actual assignment of the partition to an node.
// The reason why we separated plans with physical and logical (they can be looked like
// they could be merged at a glance) is because of the fault tolerance.
type PhysicalPlan struct {
	Key  string
	Node *node.Node
}

type PhysicalPlans []PhysicalPlan

// PhysicalPlansToMap converts a list of PhysicalPlan into mapping of partition key to hostname.
func (pp PhysicalPlans) ToMap() map[string]string {
	m := make(map[string]string, len(pp))
	for _, p := range pp {
		m[p.Key] = p.Node.Host
	}
	return m
}

// Hostnames returns an unordered list of nodes' hostnames in the partition.
func (pp PhysicalPlans) Hostnames() (nn []string) {
	for _, p := range pp {
		nn = append(nn, p.Key)
	}
	return
}

func (pp PhysicalPlans) Pretty() (s string) {
	groupsByHost := make(map[string]PhysicalPlans)
	for _, p := range pp {
		groupsByHost[p.Node.Host] = append(groupsByHost[p.Node.Host], p)
	}
	for host, plans := range groupsByHost {
		var keys []string
		for _, p := range plans {
			keys = append(keys, p.Key)
		}
		s += fmt.Sprintf(" - %s: %s\n", host, strings.Join(ellipsis(keys, 40), ", "))
	}
	return
}

func ellipsis(ss []string, maxLen int) []string {
	lenSum := 0
	for i, s := range ss {
		lenSum += len(s)
		if lenSum+len(s) > maxLen {
			return append(ss[:i], "…")
		}
	}
	return ss
}
