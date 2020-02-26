package output

import (
	"context"
	"fmt"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
)

type Output interface {
	Connect(context.Context, *node.Node, *lrmrpb.Output) error
	Send(lrdd.Row) error
	Flush() error
	Close() error
}

func NewFromDesc(outDesc *lrmrpb.Output) (Output, error) {
	switch outDesc.Type {
	case lrmrpb.Output_PARTITIONER:
		if outDesc.Partitioner.Type == lrmrpb.Partitioner_FINITE_KEY {
			return NewFiniteKeyPartitioner(), nil
		} else if outDesc.Partitioner.Type == lrmrpb.Partitioner_HASH {
			return NewHashPartitioner(), nil
		}
	case lrmrpb.Output_COLLECTOR:
		return NewCollector(), nil
	case lrmrpb.Output_ROUND_ROBIN:
		return NewRoundRobin(), nil
	case lrmrpb.Output_NONE:
		return &noneOutput{}, nil
	}
	return nil, fmt.Errorf("unsupported output: %v", outDesc.Type)
}

type noneOutput struct{}

func (n *noneOutput) Connect(context.Context, *node.Node, *lrmrpb.Output) error {
	return nil
}

func (n *noneOutput) Send(lrdd.Row) error {
	return nil
}

func (n *noneOutput) Flush() error {
	return nil
}

func (n *noneOutput) Close() error {
	return nil
}
