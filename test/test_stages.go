package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

type nopMapper struct{}

func NopMapper() lrmr.Mapper {
	return &nopMapper{}
}

func (n nopMapper) Map(_ lrmr.Context, rows []lrdd.Row) ([]lrdd.Row, error) {
	return rows, nil
}

func (n nopMapper) RowType() lrdd.RowType {
	return lrdd.RowTypeBytes
}

var _ = lrmr.RegisterTypes(
	NopMapper(),
)
