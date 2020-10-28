package partitions

import (
	"fmt"
	"sort"
	"strings"

	"github.com/thoas/go-funk"
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

	DesiredNodeAffinity map[string]string
}

// Equal returns true if the partition is equal with given partition.
// The equality is used to test dependency type of adjacent stage; If two adjacent partitions are equal,
// they are considered as narrow (local) dependency thus not involving shuffling.
func (p Plan) Equal(o Plan) bool {
	return p.DesiredCount == o.DesiredCount &&
		p.MaxNodes == o.MaxNodes &&
		p.ExecutorsPerNode == o.ExecutorsPerNode
}

// Assignment is an physical assignment of the partition to an node.
type Assignment struct {
	PartitionID string `json:"partitionID"`
	Host        string `json:"host"`
}

// Assignments is assignment of partitions in a stage.
type Assignments []Assignment

// ToMap converts a list of Assignment into mapping of partition key to hostname.
func (as Assignments) ToMap() map[string]string {
	m := make(map[string]string, len(as))
	for _, a := range as {
		m[a.PartitionID] = a.Host
	}
	return m
}

func (as Assignments) GroupIDsByHost() map[string][]string {
	m := make(map[string][]string)
	for _, a := range as {
		m[a.Host] = append(m[a.Host], a.PartitionID)
	}
	return m
}

// Keys returns an unordered list of partition keys.
func (as Assignments) Keys() (kk []string) {
	for _, a := range as {
		kk = append(kk, a.PartitionID)
	}
	return
}

// Hostnames returns an unordered list of nodes' hostnames in the partition.
func (as Assignments) Hostnames() (nn []string) {
	for _, p := range as {
		nn = append(nn, p.Host)
	}
	return
}

func (as Assignments) Pretty() (s string) {
	groupsByHost := make(map[string]Assignments)
	for _, a := range as {
		groupsByHost[a.Host] = append(groupsByHost[a.Host], a)
	}
	hosts := funk.Keys(groupsByHost).([]string)
	sort.Strings(hosts)
	for _, host := range hosts {
		var keys []string
		for _, a := range groupsByHost[host] {
			keys = append(keys, a.PartitionID)
		}
		s += fmt.Sprintf("  %s: %s\n", host, strings.Join(ellipsis(keys, 50, 500), ", "))
	}
	return
}

func ellipsis(ss []string, maxElemLen, maxLen int) []string {
	lenSum := 0
	for i, s := range ss {
		if len(s) > maxElemLen {
			s = s[:maxElemLen] + "…"
			ss[i] = s
		}
		lenSum += len(s)
		if lenSum+len(s) > maxLen {
			return append(ss[:i], "…")
		}
	}
	return ss
}
