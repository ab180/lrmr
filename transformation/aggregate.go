package transformation

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"sync"
)

type Aggregator interface {
	Transformation
	InitialValue() interface{}
	Aggregate(row lrdd.Row, currentValue interface{}) (value interface{}, err error)
}

type Aggregate struct {
	currentValue interface{}
	lock         sync.Cond
}

func (a Aggregate) Setup(c Context) error {
	a.currentValue = a.InitialValue()
	return nil
}

func (a Aggregate) Run(row lrdd.Row, out output.Output) error {
	value, err := a.Aggregate(row, a.currentValue)
	if err != nil {
		return err
	}
	a.currentValue = value
	return nil
}

func (a Aggregate) Teardown(out output.Output) error {
	return out.Send(lrdd.Row{"value": a.currentValue})
}

func (a Aggregate) InitialValue() interface{} {
	panic("implement me")
}

func (a Aggregate) Aggregate(row lrdd.Row, currentValue interface{}) (value interface{}, err error) {
	panic("implement me")
}
