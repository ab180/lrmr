package stage

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"sort"
)

type Sorter interface {
	IsLessThan(a, b *lrdd.Row) bool
}

func RegisterSorter(name string, r Sorter) (s Stage) {
	s = Stage{
		Name:    name,
		BoxType: typeOf(r),
		Constructor: func(boxed interface{}) Runner {
			return &sortStage{sorter: boxed.(Sorter)}
		},
	}
	register(s)
	return
}

type sortStage struct {
	sorter Sorter
	rows   []*lrdd.Row
}

func (s *sortStage) Setup(c Context) error {
	return nil
}

func (s *sortStage) Apply(c Context, rows []*lrdd.Row, out output.Output) error {
	s.rows = append(s.rows, rows...)
	return nil
}

func (s *sortStage) Teardown(c Context, out output.Output) error {
	sort.Sort(s)
	return out.Write(s.rows)
}

func (s *sortStage) Len() int {
	return len(s.rows)
}

func (s *sortStage) Less(i, j int) bool {
	return s.sorter.IsLessThan(s.rows[i], s.rows[j])
}

func (s *sortStage) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}
