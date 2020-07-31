package master

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/partitions"
	"github.com/therne/lrmr/transformation"
)

const CollectStageName = "_collect"

// collectResultChans stores channel of CollectedResult to gather results from ongoing jobs.
var collectResultChans sync.Map

func prepareCollect(jobID string) {
	collectResultChans.Store(jobID, make(chan []*lrdd.Row, 1))
}

func getCollectedResultChan(jobID string) (chan []*lrdd.Row, error) {
	v, ok := collectResultChans.Load(jobID)
	if !ok {
		return nil, errors.Errorf("unknown job: %s", jobID)
	}
	return v.(chan []*lrdd.Row), nil
}

type Collector struct{}

func (c *Collector) Apply(ctx transformation.Context, in chan *lrdd.Row, _ output.Output) error {
	resultChan, err := getCollectedResultChan(ctx.JobID())
	if err != nil {
		return errors.Errorf("unknown job: %s", ctx.JobID())
	}
	var rows []*lrdd.Row
	for row := range in {
		rows = append(rows, row)
	}
	resultChan <- rows
	collectResultChans.Delete(ctx.JobID())
	return nil
}

type CollectPartitioner struct{}

func NewCollectPartitioner() partitions.Partitioner {
	return &CollectPartitioner{}
}

// PlanNext assigns partition to master.
func (c CollectPartitioner) PlanNext(numExecutors int) []partitions.Partition {
	assignToMaster := map[string]string{
		"Type": string(node.Master),
	}
	return []partitions.Partition{
		{ID: "_collect", AssignmentAffinity: assignToMaster},
	}
}

// DeterminePartition always partitions data to "_collect" partition.
func (c CollectPartitioner) DeterminePartition(partitions.Context, *lrdd.Row, int) (id string, err error) {
	return "_collect", nil
}
