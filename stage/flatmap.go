package stage

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"sync"
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
	wg sync.WaitGroup
}

func (fs *flatMapStage) Setup(c Context) error {
	if b, ok := fs.fm.(Bootstrapper); ok {
		return b.Setup(c)
	}
	return nil
}

func (fs *flatMapStage) Apply(c Context, in *lrdd.Row, out output.Writer) error {
	fs.wg.Add(1)
	c.Spawn(func() (err error) {
		defer fs.wg.Done()

		results, err := fs.fm.FlatMap(c, in)
		if err != nil {
			return err
		}
		for _, result := range results {
			if err := out.Write(result); err != nil {
				return err
			}
		}
		return nil
	})
	return nil
}

func (fs *flatMapStage) Teardown(c Context, out output.Writer) error {
	fs.wg.Wait()
	if b, ok := fs.fm.(Bootstrapper); ok {
		return b.Teardown(c, out)
	}
	return nil
}
