package stage

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
)

type FlatMapper interface {
	FlatMap(Context, *lrdd.Row) ([]*lrdd.Row, error)
}

func RegisterFlatMap(name string, fm FlatMapper) (s Stage) {
	s = Stage{
		Name:    name,
		BoxType: typeOf(fm),
		Constructor: func(boxed interface{}) Runner {
			return &flatMapStage{fm: boxed.(FlatMapper)}
		},
	}
	register(s)
	return
}

type flatMapStage struct {
	fm FlatMapper
}

func (fs *flatMapStage) Setup(c Context) error {
	if b, ok := fs.fm.(Bootstrapper); ok {
		return b.Setup(c)
	}
	return nil
}

func (fs *flatMapStage) Apply(c Context, rows []*lrdd.Row, out output.Output) error {
	for _, row := range rows {
		rows, err := fs.fm.FlatMap(c, row)
		if err != nil {
			return err
		}
		if err := out.Write(rows); err != nil {
			return err
		}
	}
	return nil
}

func (fs *flatMapStage) Teardown(c Context, out output.Output) error {
	if b, ok := fs.fm.(Bootstrapper); ok {
		return b.Teardown(c, out)
	}
	return nil
}
