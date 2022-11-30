package test

import (
	"strconv"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
	partitions "github.com/ab180/lrmr/partitions"
)

var _ = lrmr.RegisterTypes(&dummyMapper{}, &nodeAffinityTester{})

func PartitionerWithNodeAffinityTest() *lrmr.Pipeline {
	in := map[string]string{
		"key1-1": "",
		"key1-2": "",
		"key2-1": "",
		"key2-2": "",
	}
	return lrmr.Parallelize(lrdd.FromStringMap(in)).
		PartitionedBy(&nodeAffinityTester{}).
		Map(&dummyMapper{})
}

type nodeAffinityTester struct{}

func (c nodeAffinityTester) DeterminePartition(ctx partitions.Context, r *lrdd.Row, numOutputs int,
) (id string, err error) {
	return r.Key, nil
}

func (c nodeAffinityTester) PlanNext(numExecutors int) []partitions.Partition {
	return []partitions.Partition{
		{ID: "key1-1", AssignmentAffinity: map[string]string{"No": "1"}},
		{ID: "key1-2", AssignmentAffinity: map[string]string{"No": "1"}},
		{ID: "key2-1", AssignmentAffinity: map[string]string{"No": "2"}},
		{ID: "key2-2", AssignmentAffinity: map[string]string{"No": "2"}},
	}
}

type dummyMapper struct{}

func (d *dummyMapper) Map(ctx lrmr.Context, row []*lrdd.Row) ([]*lrdd.Row, error) {
	mappedRows := make([]*lrdd.Row, len(row))
	for i, row := range row {
		workerNo := ctx.WorkerLocalOption("No").(int)
		mappedRows[i] = &lrdd.Row{Key: strconv.Itoa(workerNo), Value: []byte(row.Key)}
	}

	return mappedRows, nil
}
