package stage

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
)

type Reducer interface {
	InitialValue() interface{}
	Reduce(c Context, prev interface{}, cur lrdd.Row) (next interface{}, err error)
}

func RegisterReducer(name string, r Reducer) (s Stage) {
	s = Stage{
		Name:    name,
		BoxType: typeOf(r),
		Constructor: func(boxed interface{}) Runner {
			return &reduceStage{r: boxed.(Reducer)}
		},
	}
	register(s)
	return
}

type reduceStage struct {
	r    Reducer
	Prev interface{}
}

func (rs *reduceStage) Setup(c Context) error {
	rs.Prev = rs.r.InitialValue()
	if b, ok := rs.r.(Bootstrapper); ok {
		return b.Setup(c)
	}
	return nil
}

func (rs *reduceStage) Apply(c Context, in lrdd.Row, out output.Writer) error {
	next, err := rs.r.Reduce(c, rs.Prev, in)
	if err != nil {
		return err
	}
	rs.Prev = next
	return nil
}

func (rs *reduceStage) Teardown(c Context, out output.Writer) error {
	if err := out.Write(lrdd.KeyValue(c.PartitionKey(), rs.Prev)); err != nil {
		return err
	}
	if b, ok := rs.r.(Bootstrapper); ok {
		return b.Teardown(c, out)
	}
	return nil
}
