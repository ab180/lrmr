package stage

import (
	"fmt"
	"github.com/modern-go/reflect2"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"reflect"
)

type Runner interface {
	Setup(c Context) error
	Apply(c Context, row lrdd.Row, out output.Writer) error
	Teardown(c Context) error
}

type RunFunc func(c Context, row lrdd.Row, out output.Writer) error

var typeToStage = map[string]Stage{}

func typeOf(tf Runner) reflect.Type {
	t := reflect.TypeOf(tf)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func LookupByRunner(r Runner) Stage {
	typ := typeOf(r)
	name := typ.PkgPath() + "." + typ.Name()
	s, ok := typeToStage[name]
	if !ok {
		msg := fmt.Sprintf("stage %s does not exist. does it registered on both master and worker?", name)
		panic(msg)
	}
	return s
}

func Register(name string, r Runner) Stage {
	if _, exists := registry[name]; exists {
		panic("stage " + name + " have been already registered.")
	}
	typ := typeOf(r)
	stage := Stage{
		Name: name,
		Type: typ,
		Constructor: func() Runner {
			return reflect2.Type2(typ).New().(Runner)
		},
	}
	registry[name] = stage
	typeToStage[typ.PkgPath()+"."+typ.Name()] = stage
	return stage
}

func RegisterFunc(name string, fn RunFunc) Stage {
	if _, exists := registry[name]; exists {
		panic("stage " + name + " have been already registered.")
	}
	stage := Stage{
		Name: name,
		Type: funcWrapperType,
		Constructor: func() Runner {
			return &runFuncWrapper{fn: fn}
		},
	}
	registry[name] = stage
	return stage
}

var funcWrapperType = reflect.TypeOf(&runFuncWrapper{})

type runFuncWrapper struct {
	Simple
	fn RunFunc
}

func (w *runFuncWrapper) Apply(c Context, row lrdd.Row, out output.Writer) error {
	return w.fn(c, row, out)
}
