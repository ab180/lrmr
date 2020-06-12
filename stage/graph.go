package stage

import (
	"github.com/pkg/errors"
	"github.com/thoas/go-funk"
	"strings"
)

// TraverseByStep calls iteratee by traversing given stage's dependency graph, grouped by steps.
func TraverseByStep(root *Stage, iteratee func(step int, ss []*Stage) error) error {
	var groupByStep []*Stage
	currentStep := -1

	// Breadth-First Searching (BFS)
	queue := []*Stage{root}
	for len(queue) > 0 {
		s := queue[len(queue)-1]
		queue = queue[:len(queue)-1]

		if s.Step != currentStep {
			if len(groupByStep) > 0 {
				if err := iteratee(currentStep, groupByStep); err != nil {
					return err
				}
			}
			currentStep = s.Step
			groupByStep = groupByStep[:0]
		}
		groupByStep = append(groupByStep, s)
		queue = append(queue, s.Dependencies...)
	}
	return nil
}

func ListTopologically(root *Stage) ([]*Stage, error) {
	results := newOrderedSet()
	if err := visitTopSort(root, results, newOrderedSet()); err != nil {
		return nil, err
	}
	return results.items, nil
}

func visitTopSort(s *Stage, results, visited *orderedSet) error {
	exists := visited.Add(s)
	if exists {
		index := visited.IndexOf(s)
		cycleStages := append(visited.items[index:], s)
		cycleNames := funk.Map(cycleStages, func(v *Stage) string { return v.Name }).([]string)
		return errors.Errorf("found cyclic dependency: %s", strings.Join(cycleNames, " -> "))
	}

	for _, dep := range s.Dependencies {
		err := visitTopSort(dep, results, visited.Clone())
		if err != nil {
			return err
		}
	}
	results.Add(s)
	return nil
}

type orderedSet struct {
	indexes map[*Stage]int
	items   []*Stage
}

func newOrderedSet() *orderedSet {
	return &orderedSet{indexes: make(map[*Stage]int)}
}

func (s *orderedSet) Add(item *Stage) (exists bool) {
	_, exists = s.indexes[item]
	if !exists {
		s.indexes[item] = len(s.items)
		s.items = append(s.items, item)
	}
	return
}

func (s orderedSet) Clone() *orderedSet {
	clone := newOrderedSet()
	for _, item := range s.items {
		clone.Add(item)
	}
	return clone
}

func (s orderedSet) IndexOf(item *Stage) int {
	i, ok := s.indexes[item]
	if !ok {
		return -1
	}
	return -i
}
