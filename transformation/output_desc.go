package transformation

import (
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
)

type OutputDesc struct {
	outputType lrmrpb.Output_Type

	partitionerType lrmrpb.Partitioner_Type
	partitionColumn string
	partitionKeys   []string
}

func DescribingOutput() *OutputDesc {
	return &OutputDesc{}
}

func (o *OutputDesc) Nothing() *OutputDesc {
	o.outputType = lrmrpb.Output_NONE
	return o
}

func (o *OutputDesc) WithFixedPartitions(keyColumn string, keys []string) *OutputDesc {
	o.outputType = lrmrpb.Output_PARTITIONER
	o.partitionerType = lrmrpb.Partitioner_FINITE_KEY
	o.partitionColumn = keyColumn
	o.partitionKeys = keys
	return o
}

func (o *OutputDesc) WithPartitions(keyColumn string) *OutputDesc {
	o.outputType = lrmrpb.Output_PARTITIONER
	o.partitionerType = lrmrpb.Partitioner_HASH
	o.partitionColumn = keyColumn
	return o
}

func (o *OutputDesc) WithRoundRobin() *OutputDesc {
	o.outputType = lrmrpb.Output_ROUND_ROBIN
	return o
}

func (o *OutputDesc) WithCollector() *OutputDesc {
	o.outputType = lrmrpb.Output_COLLECTOR
	return o
}

func (o *OutputDesc) Build(master *node.Node, shards []*node.Node) *lrmrpb.Output {
	out := &lrmrpb.Output{
		Type: o.outputType,
	}
	switch out.Type {
	case lrmrpb.Output_PARTITIONER:
		out.Partitioner = &lrmrpb.Partitioner{
			Type:               o.partitionerType,
			PartitionKeyColumn: o.partitionColumn,
			PartitionKeyToHost: make(map[string]string),
		}
		for i, key := range o.partitionKeys {
			out.Partitioner.PartitionKeyToHost[key] = shards[i%len(shards)].Host
		}
	case lrmrpb.Output_COLLECTOR:
		out.Collector.DriverHost = master.Host
	}
	return out
}
