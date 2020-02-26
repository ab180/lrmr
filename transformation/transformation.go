package transformation

import (
	"fmt"
	"github.com/modern-go/reflect2"
	"github.com/therne/lrmr/dataframe"
	"github.com/therne/lrmr/output"
	"reflect"
	"strings"
)

var registry = make(map[string]reflect2.Type)

type Context interface {
	Broadcast(key string) interface{}
}

type Transformation interface {
	DescribeOutput() *OutputDesc

	Setup(c Context) error
	Run(row dataframe.Row, out output.Output) error
	Teardown() error
}

func typeOf(tf Transformation) reflect.Type {
	t := reflect.TypeOf(tf)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t
}

func NameOf(tf Transformation) string {
	t := typeOf(tf)
	return t.PkgPath() + "." + t.Name()
}

func Register(tf Transformation) {
	registry[NameOf(tf)] = reflect2.Type2(typeOf(tf))
}

func Lookup(id string) Transformation {
	if typ, ok := registry[id]; ok {
		return typ.New().(Transformation)
	}
	// fallback: reflect lookup
	frags := strings.Split(id, ".")
	pkgPath := strings.Join(frags[0:len(frags)-1], ".")
	typ := reflect2.TypeByPackageName(pkgPath, frags[len(frags)-1])
	if typ == nil {
		msg := fmt.Sprintf("transformation %s does not exist. does it registered on both master and worker?", id)
		panic(msg)
	}
	return typ.New().(Transformation)
}
