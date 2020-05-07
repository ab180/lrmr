package partitions

import (
	"fmt"
	"github.com/therne/lrmr/node"
	"strings"
)

const (
	Auto = 0
	None = -1
)

type Plan struct {
	Partitioner      Partitioner
	DesiredCount     int
	MaxNodes         int
	ExecutorsPerNode int
}

// Equal returns true if the partition is equal with given partition.
// The equality is used to test dependency type of adjacent stage; If two adjacent partitions are equal,
// they are considered as narrow (local) dependency thus not involving shuffling.
func (p Plan) Equal(o Plan) bool {
	return p.Partitioner == o.Partitioner &&
		p.DesiredCount == o.DesiredCount &&
		p.MaxNodes == o.MaxNodes &&
		p.ExecutorsPerNode == o.ExecutorsPerNode
}

// Assignment is an physical assignment of the partition to an node.
type Assignment struct {
	Partition
	Node *node.Node
}

type Assignments []Assignment

// ToMap converts a list of Assignment into mapping of partition key to hostname.
func (pp Assignments) ToMap() map[string]string {
	m := make(map[string]string, len(pp))
	for _, p := range pp {
		m[p.ID] = p.Node.Host
	}
	return m
}

// Keys returns an unordered list of partition keys.
func (pp Assignments) Keys() (kk []string) {
	for _, p := range pp {
		kk = append(kk, p.ID)
	}
	return
}

// Hostnames returns an unordered list of nodes' hostnames in the partition.
func (pp Assignments) Hostnames() (nn []string) {
	for _, p := range pp {
		nn = append(nn, p.Node.Host)
	}
	return
}

func (pp Assignments) Pretty() (s string) {
	groupsByHost := make(map[string]Assignments)
	for _, p := range pp {
		groupsByHost[p.Node.Host] = append(groupsByHost[p.Node.Host], p)
	}
	for host, plans := range groupsByHost {
		var keys []string
		for _, p := range plans {
			keys = append(keys, p.ID)
		}
		s += fmt.Sprintf(" - %s: %s\n", host, strings.Join(ellipsis(keys, 500), ", "))
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
