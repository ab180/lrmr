package test

import (
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/job/partitions"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/stage"
	"strconv"
)

func CustomPartitionerTest(m *master.Master) *lrmr.Dataset {
	stage.RegisterMap("dummyMapper", &dummyMapper{})

	in := map[string]string{
		"key1-1": "",
		"key1-2": "",
		"key2-1": "",
		"key2-2": "",
	}
	return lrmr.Parallelize(in, m).
		PartitionedBy(&customPlanner{}).
		Map(&dummyMapper{})
}

type customPlanner struct{}

func (c customPlanner) Plan() partitions.LogicalPlans {
	return partitions.LogicalPlans{
		{Key: "key1-1", NodeAffinityRules: map[string]string{"No": "1"}},
		{Key: "key1-2", NodeAffinityRules: map[string]string{"No": "1"}},
		{Key: "key2-1", NodeAffinityRules: map[string]string{"No": "2"}},
		{Key: "key2-2", NodeAffinityRules: map[string]string{"No": "2"}},
	}
}

type dummyMapper struct{}

func (d *dummyMapper) Map(ctx stage.Context, row *lrdd.Row) (*lrdd.Row, error) {
	workerNo := ctx.WorkerLocalOption("No").(int)
	return lrdd.KeyValue(strconv.Itoa(workerNo), row.Key), nil
}
