package transformation

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
)

type Mapper interface {
	Transformation
	Map(lrdd.Row) (lrdd.Row, error)
}

type Map struct {
	Simple
}

func (m *Map) Apply(row lrdd.Row, out output.Output, executorID int) error {
	result, err := m.Map(row)
	if err != nil {
		return err
	}
	return out.Send(result)
}

func (m *Map) Map(row lrdd.Row) (lrdd.Row, error) {
	panic("implement me")
}
