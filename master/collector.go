package master

import (
	"sync"

	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/output"
	"github.com/ab180/lrmr/partitions"
	"github.com/ab180/lrmr/transformation"
	"github.com/pkg/errors"
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
	resultChan, err := getCollectedResultChan(ctx.Job().ID)
	if err != nil {
		return errors.Errorf("unknown job: %s", ctx.Job().ID)
	}
	var rows []*lrdd.Row
	for row := range in {
		rows = append(rows, row)
	}
	resultChan <- rows
	collectResultChans.Delete(ctx.Job().ID)
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
