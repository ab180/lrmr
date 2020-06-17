package test

import (
	"strconv"

	"github.com/therne/lrmr"
	"github.com/therne/lrmr/lrdd"
	partitions "github.com/therne/lrmr/partitions"
)

var _ = lrmr.RegisterTypes(&dummyMapper{}, &nodeAffinityTester{})

func PartitionerWithNodeAffinityTest(sess *lrmr.Session) *lrmr.Dataset {
	in := map[string]string{
		"key1-1": "",
		"key1-2": "",
		"key2-1": "",
		"key2-2": "",
	}
	return sess.Parallelize(in).
		PartitionedBy(&nodeAffinityTester{}).
		Map(&dummyMapper{})
}

type nodeAffinityTester struct{}

func (c nodeAffinityTester) DeterminePartition(ctx partitions.Context, r *lrdd.Row, numOutputs int) (id string, err error) {
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

func (d *dummyMapper) Map(ctx lrmr.Context, row *lrdd.Row) (*lrdd.Row, error) {
	workerNo := ctx.WorkerLocalOption("No").(int)
	return lrdd.KeyValue(strconv.Itoa(workerNo), row.Key), nil
}
