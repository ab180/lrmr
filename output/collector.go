package output

import (
	"context"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
)

type Collector struct {
}

func NewCollector() Output {
	return &Collector{}
}

func (c *Collector) Connect(context.Context, *node.Node, *lrmrpb.Output) error {
	panic("implement me")
}

func (c *Collector) Send(lrdd.Row) error {
	panic("implement me")
}

func (c *Collector) Flush() error {
	panic("implement me")
}

func (c *Collector) Close() error {
	panic("implement me")
}
