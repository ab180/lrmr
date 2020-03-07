package stage

import (
	"fmt"
	"reflect"
)

type Stage struct {
	Name   string
	Type   reflect.Type
	Input  reflect.Type
	Output reflect.Type

	Constructor func() Runner
}

var registry = map[string]Stage{}

func Lookup(name string) Stage {
	s, ok := registry[name]
	if !ok {
		msg := fmt.Sprintf("stage %s does not exist. does it registered on both master and worker?", name)
		panic(msg)
	}
	return s
}
