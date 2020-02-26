package transformation

import (
	"github.com/therne/lrmr/dataframe"
	"github.com/therne/lrmr/output"
)

type Simple struct{}

func (s *Simple) DescribeOutput() *OutputDesc {
	panic("implement me")
}

func (s *Simple) Run(row dataframe.Row, out output.Output) error {
	panic("implement me")
}

func (s *Simple) Setup(c Context) error {
	return nil
}

func (s *Simple) Teardown() error {
	return nil
}

var _ Transformation = &Simple{}
