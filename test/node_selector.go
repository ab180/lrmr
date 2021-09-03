package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

func NodeSelection() *lrmr.Pipeline {
	return lrmr.Parallelize([]int{}).
		Do(countNumPartitions{})
}

type countNumPartitions struct{}

func (c countNumPartitions) Transform(ctx lrmr.Context, in chan *lrdd.Row, emit func(*lrdd.Row)) error {
	ctx.AddMetric("NumPartitions", 1)
	<-in
	return nil
}

var _ = lrmr.RegisterTypes(countNumPartitions{})
