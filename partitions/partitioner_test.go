package partitions

import (
	"sort"
	"testing"

	"github.com/ab180/lrmr/cluster/node"
	"github.com/stretchr/testify/require"
)

func TestPartitioner(t *testing.T) {
	ns := []*node.Node{
		{Host: "localhost:1001", Executors: 1},
		{Host: "localhost:1002", Executors: 1},
		{Host: "localhost:1003", Executors: 1},
		{Host: "localhost:1004", Executors: 1},
	}

	tcs := []struct {
		Partitioner         Partitioner
		ExpectedAssignments Assignments
	}{
		{
			Partitioner: NewPreservePartitioner(),
			ExpectedAssignments: Assignments{
				{Host: "localhost:1001", PartitionID: "3"},
				{Host: "localhost:1002", PartitionID: "0"},
				{Host: "localhost:1003", PartitionID: "1"},
				{Host: "localhost:1004", PartitionID: "2"},
			},
		},
	}

	for _, tc := range tcs {
		const secondPlanIdx = 1

		_, ass := Schedule(
			ns,
			[]Plan{
				{
					// Ignore the first plan.
				},
				{
					Partitioner: tc.Partitioner,
				},
			})

		as := ass[secondPlanIdx]

		sort.Slice(as, func(i, j int) bool {
			return as[i].Host < as[j].Host
		})

		require.Equal(t, tc.ExpectedAssignments, as)
	}
}
