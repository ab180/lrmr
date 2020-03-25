package stage

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
)

type Bootstrapper interface {
	Setup(c Context) error
	Teardown(c Context, out output.Output) error
}

type Runner interface {
	Bootstrapper
	Apply(c Context, rows []*lrdd.Row, out output.Output) error
}

func Register(name string, r Runner) Stage {
	if _, exists := nameToStage[name]; exists {
		panic("stage " + name + " have been already registered.")
	}
	typ := typeOf(r)
	stage := Stage{
		Name:    name,
		BoxType: typ,
		Constructor: func(box interface{}) Runner {
			return box.(Runner)
		},
	}
	register(stage)
	return stage
}
