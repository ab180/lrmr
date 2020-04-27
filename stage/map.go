package stage

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
)

type Mapper interface {
	Map(Context, *lrdd.Row) (*lrdd.Row, error)
}

func RegisterMap(name string, m Mapper) (s Stage) {
	s = Stage{
		Name:    name,
		BoxType: typeOf(m),
		Constructor: func(boxed interface{}) Runner {
			return &mapStage{boxed.(Mapper)}
		},
	}
	register(s)
	return
}

type mapStage struct {
	mapper Mapper
}

func (fs *mapStage) Setup(c Context) error {
	if b, ok := fs.mapper.(Bootstrapper); ok {
		return b.Setup(c)
	}
	return nil
}

func (fs *mapStage) Apply(c Context, rows []*lrdd.Row, out output.Output) (err error) {
	for i, row := range rows {
		// reuse `rows` for zero memory allocation trick :P
		rows[i], err = fs.mapper.Map(c, row)
		if err != nil {
			return err
		}
	}
	return out.Write(rows)
}

func (fs *mapStage) Teardown(c Context, out output.Output) error {
	if b, ok := fs.mapper.(Bootstrapper); ok {
		return b.Teardown(c, out)
	}
	return nil
}
