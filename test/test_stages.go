package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
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
