package lrmr

import (
	"github.com/modern-go/reflect2"
	"path"
	"reflect"
)

type Plan struct {
	Complexity uint64
	Key        string
}

type Task interface {
	Plan(c Context) ([]Plan, error)

	Setup(c Context) error
	Run(val interface{}, output map[string]chan interface{}) error
	Teardown() error
}

func LookupTaskBy(name string) Task {
	pkgPath, typeName := path.Split(name)
	typ := reflect2.TypeByPackageName(pkgPath, typeName)
	if typ == nil {
		panic("task not found: " + name + " (maybe it is unused and purged by compiler)")
	}
	return InstantiateTask(typ)
}

func InstantiateTask(typ reflect.Type) Task {
	instance := reflect.New(typ).Interface()
	task, ok := instance.(Task)
	if !ok {
		panic(typ.String() + "does not implement lrmr.Task")
	}
	return task
}
