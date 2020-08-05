package test

import (
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/lrdd"
)

type nopMapper struct{}

func NopMapper() lrmr.Mapper {
	return &nopMapper{}
}

func (n nopMapper) Map(_ lrmr.Context, row *lrdd.Row) (*lrdd.Row, error) {
	return row, nil
}

var _ = lrmr.RegisterTypes(
	NopMapper(),
)
