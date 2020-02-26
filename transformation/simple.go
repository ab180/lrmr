package transformation

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
)

type Simple struct{}

func (s *Simple) Run(row lrdd.Row, out output.Output) error {
	panic("implement me")
}

func (s *Simple) Setup(c Context) error {
	return nil
}

func (s *Simple) Teardown(out output.Output) error {
	return nil
}

var _ Transformation = &Simple{}
