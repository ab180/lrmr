package transformation

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
)

type Simple struct{}

func (s *Simple) Apply(c Context, row lrdd.Row, out output.Writer) error {
	panic("implement me")
}

func (s *Simple) Setup(c Context) error {
	return nil
}

func (s *Simple) Teardown(c Context) error {
	return nil
}

var _ Transformation = &Simple{}
