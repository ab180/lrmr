package executor

import (
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/output"
	"github.com/ab180/lrmr/partitions"
)

const (
	CollectStageName   = "__collect"
	collectPartitionID = "__collect"
)

type Collector struct {
	reporter StatusReporter
}

func NewCollector(reporter StatusReporter) *Collector {
	return &Collector{
		reporter: reporter,
	}
}

func (c *Collector) Write(rows []lrdd.Row) error {
	return c.reporter.Collect(rows)
}

func (c *Collector) Close() error {
	return nil
}

type collectPartitioner struct{}

func (collectPartitioner) PlanNext(int) []partitions.Partition {
	return partitions.PlanForNumberOf(1)
}

func (collectPartitioner) DeterminePartition(partitions.Context, lrdd.Row, int) (id string, err error) {
	return collectPartitionID, nil
}

var (
	_ output.Output          = (*Collector)(nil)
	_ partitions.Partitioner = (*collectPartitioner)(nil)
)
