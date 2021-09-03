package executor

import (
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/lrmrpb"
	"github.com/ab180/lrmr/output"
	"github.com/ab180/lrmr/partitions"
)

const (
	collectStageName   = "__collect"
	collectPartitionID = "__collect"
)

type Collector struct {
	stream lrmrpb.Node_RunJobInForegroundServer
}

func NewCollector(stream lrmrpb.Node_RunJobInForegroundServer) *Collector {
	return &Collector{
		stream: stream,
	}
}

func (c *Collector) PlanNext(int) []partitions.Partition {
	return partitions.PlanForNumberOf(1)
}

func (c *Collector) DeterminePartition(partitions.Context, *lrdd.Row, int) (id string, err error) {
	return collectPartitionID, nil
}

func (c *Collector) Write(rows ...*lrdd.Row) error {
	return c.stream.Send(&lrmrpb.RunOnlineJobOutputToDriver{
		Type: lrmrpb.RunOnlineJobOutputToDriver_COLLECT_DATA,
		Data: rows,
	})
}

func (c *Collector) Close() error {
	return nil
}

var (
	_ partitions.Partitioner = (*Collector)(nil)
	_ output.Output          = (*Collector)(nil)
)
