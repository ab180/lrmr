package stage

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
)

type Filter interface {
	Predicate(*lrdd.Row) bool
}

func RegisterFilter(name string, f Filter) (s Stage) {
	s = Stage{
		Name:    name,
		BoxType: typeOf(f),
		Constructor: func(boxed interface{}) Runner {
			return &filterStage{f: boxed.(Filter)}
		},
	}
	register(s)
	return
}

type filterStage struct {
	f Filter
}

func (fs *filterStage) Setup(c Context) error {
	if b, ok := fs.f.(Bootstrapper); ok {
		return b.Setup(c)
	}
	return nil
}

func (fs *filterStage) Apply(c Context, rows []*lrdd.Row, out output.Output) error {
	// filtering without additional memory allocation :P
	// https://github.com/golang/go/wiki/SliceTricks#filtering-without-allocating
	results := rows[:0]
	for _, row := range rows {
		if !fs.f.Predicate(row) {
			continue
		}
		results = append(results, row)
	}
	for i := len(results); i < len(rows); i++ {
		// garbage collect filtered elements
		rows[i] = nil
	}
	return out.Write(results)
}

func (fs *filterStage) Teardown(c Context, out output.Output) error {
	if b, ok := fs.f.(Bootstrapper); ok {
		return b.Teardown(c, out)
	}
	return nil
}
