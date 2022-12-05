package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

func NodeSelection(selector map[string]string) *lrmr.Pipeline {
	return lrmr.Parallelize(nil, lrmr.WithNodeSelector(selector)).
		Do(countNumPartitions{})
}

type countNumPartitions struct{}

func (c countNumPartitions) Transform(ctx lrmr.Context, in chan []*lrdd.Row, emit lrmr.EmitFunc) error {
	ctx.AddMetric("NumPartitions", 1)
	<-in
	return nil
}

func (c countNumPartitions) RowType() lrdd.RowType {
	return lrdd.RowTypeBytes
}

var _ = lrmr.RegisterTypes(countNumPartitions{})
