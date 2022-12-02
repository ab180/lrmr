package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

type nopMapper struct{}

func NopMapper() lrmr.Mapper {
	return &nopMapper{}
}

func (n nopMapper) Map(_ lrmr.Context, rows []*lrdd.Row) ([]*lrdd.Row, error) {
	return rows, nil
}

func (n nopMapper) RowID() lrdd.RowID {
	return lrdd.RowIDBytes
}

var _ = lrmr.RegisterTypes(
	NopMapper(),
)
