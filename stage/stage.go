package stage

import (
	"fmt"
	"github.com/modern-go/reflect2"
	"reflect"
)

var nameToStage = map[string]Stage{}
var typeToStage = map[string]Stage{}

type Stage struct {
	Name   string
	Input  reflect.Type
	Output reflect.Type

	// BoxType is the original (before boxed with Runner) type of the stage.
	BoxType reflect.Type

	Constructor func(boxed interface{}) Runner
}

func (st Stage) NewBox() interface{} {
	return reflect2.Type2(st.BoxType).New()
}

func typeOf(v interface{}) reflect.Type {
	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func typeName(t reflect.Type) string {
	return t.PkgPath() + "." + t.Name()
}

func Lookup(name string) Stage {
	s, ok := nameToStage[name]
	if !ok {
		msg := fmt.Sprintf("stage %s does not exist. does it registered on both master and worker?", name)
		panic(msg)
	}
	return s
}

func LookupByRunner(v interface{}) Stage {
	typ := typeOf(v)
	s, ok := typeToStage[typeName(typ)]
	if !ok {
		msg := fmt.Sprintf("stage %s does not exist. does it registered on both master and worker?", typeName(typ))
		panic(msg)
	}
	return s
}

func register(s Stage) {
	if _, exists := nameToStage[s.Name]; exists {
		panic(s.Name + " already exists")
	}
	nameToStage[s.Name] = s
	typeToStage[typeName(s.BoxType)] = s
}
