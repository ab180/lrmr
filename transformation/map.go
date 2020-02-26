package transformation

import (
	"github.com/therne/lrmr/dataframe"
	"github.com/therne/lrmr/output"
)

type Map struct {
	Simple
}

func (m *Map) DescribeOutput() *OutputDesc {
	return DescribingOutput().WithRoundRobin()
}

func (m *Map) Run(row dataframe.Row, out output.Output) error {
	result, err := m.Map(row)
	if err != nil {
		return err
	}
	return out.Send(result)
}

func (m *Map) Map(row dataframe.Row) (dataframe.Row, error) {
	panic("implement me")
}
