package serialization

import (
	"reflect"
	"strings"
	"sync"

	"github.com/modern-go/reflect2"
	"github.com/pkg/errors"
)

var cache sync.Map

// Type wraps reflect.Type with serialization support.
// To deserialize the type on the remote, it also needs be available and registered in the remote side.
type Type struct {
	T reflect2.Type `json:"-"`
}

// TypeOf returns a wrapped type of given value. It also has a side-effect that registers given type,
// which allows deserialization of the serialized given type on this process/application.
func TypeOf(v interface{}) Type {
	// calling reflect2.TypeOf registers given type to the reflect2 cache
	t := Type{reflect2.TypeOf(v)}

	cache.Store(t.String(), t)
	return t
}

// TypeFromString loads and returns type from given type descriptor.
// type descriptor is composed of <kind><pkgPath>.<typeName> (e.g. []*github.com/pkg/errors.Error).
// It returns ErrUnresolved if given type is not found on this process/application.
func TypeFromString(desc string) (Type, error) {
	if v, hit := cache.Load(desc); hit {
		return v.(Type), nil
	}
	t, err := deserializeType(desc)
	if err != nil {
		return Type{}, err
	}
	typ := Type{T: reflect2.Type2(t)}
	cache.Store(desc, typ)
	return typ, nil
}

// New returns a pointer to data of this type.
func (t Type) New() interface{} {
	return t.T.New()
}

func (t Type) IsSameType(v interface{}) bool {
	return t.T == reflect2.TypeOf(v)
}

func (t Type) String() string {
	if t.T == nil {
		return "nil"
	}
	return serializeTypeInfo(t.T.Type1())
}

func (t Type) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t *Type) UnmarshalText(d []byte) (err error) {
	*t, err = TypeFromString(string(d))
	if err != nil {
		return nil
	}
	return nil
}

func serializeTypeInfo(typ reflect.Type) string {
	if typ == nil {
		return "nil"
	}
	switch typ.Kind() {
	case reflect.Slice:
		return "[]" + serializeTypeInfo(typ.Elem())
	case reflect.Ptr:
		return "*" + serializeTypeInfo(typ.Elem())
	default:
		pkg := typ.PkgPath()
		if pkg == "" {
			// probably primitives
			return typ.Name()
		}
		return pkg + "." + typ.Name()
	}
}

func deserializeType(typ string) (reflect.Type, error) {
	switch {
	case strings.HasPrefix(typ, "[]"):
		elemTyp, err := deserializeType(typ[2:])
		if err != nil {
			return nil, err
		}
		return reflect.SliceOf(elemTyp), nil

	case strings.HasPrefix(typ, "*"):
		elemTyp, err := deserializeType(typ[1:])
		if err != nil {
			return nil, err
		}
		return reflect.PtrTo(elemTyp), nil

	default:
		if v, hit := cache.Load(typ); hit {
			// primitives / wrapped
			return v.(Type).T.Type1(), nil
		}
		dotIndex := strings.LastIndex(typ, ".")
		if dotIndex == -1 {
			return nil, errors.Errorf("invalid type: %s", typ)
		}
		pkg, name := typ[:dotIndex], typ[dotIndex+1:]

		t := reflect2.TypeByPackageName(pkg, name)
		if t == nil {
			return nil, errors.Wrapf(ErrUnresolved, "resolve %s", typ)
		}
		return t.Type1(), nil
	}
}

// ErrUnresolved is returned when the type with given package path and name does not exist.
// It usually caused by unuse; Go compiler erases unused and unimported types, so you need to ensure that
// receiver of the serialized struct imports the referred type.
var ErrUnresolved = errors.New("unknown type")

// register primitive types
var (
	_ = TypeOf(nil)
	_ = TypeOf(0)
	_ = TypeOf("")
	_ = TypeOf(true)
	_ = TypeOf(byte(0))
	_ = TypeOf(rune(0))
	_ = TypeOf(int8(0))
	_ = TypeOf(int16(0))
	_ = TypeOf(int32(0))
	_ = TypeOf(int64(0))
	_ = TypeOf(uint8(0))
	_ = TypeOf(uint16(0))
	_ = TypeOf(uint32(0))
	_ = TypeOf(uint64(0))
	_ = TypeOf(float32(0))
	_ = TypeOf(float64(0))
	_ = TypeOf(complex64(0))
	_ = TypeOf(complex128(0))
)
