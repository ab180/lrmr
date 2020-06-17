package serialization

import (
	"bytes"
	"reflect"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
	"github.com/pkg/errors"
)

// ErrUnresolved is returned when the type with given package path and name does not exist.
// It usually caused by unuse; Go compiler erases unused and unimported types, so you need to ensure that
// receiver of the serialized struct imports the referred type.
var ErrUnresolved = errors.New("unresolved type")

type StructDesc struct {
	Type string      `json:"@type"`
	Data interface{} `json:"data"`
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
		return typ.PkgPath() + "." + typ.Name()
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
		sepIdx := strings.LastIndex(typ, ".")
		if sepIdx == -1 {
			return nil, errors.Errorf("invalid type descriptor: %s", typ)
		}
		pkg, name := typ[:sepIdx], typ[sepIdx+1:]

		var t reflect2.Type
		if pkg == "" {
			// primitives
			t = reflect2.TypeByName(name)
		} else {
			t = reflect2.TypeByPackageName(pkg, name)
		}
		if t == nil {
			return nil, errors.Wrapf(ErrUnresolved, "resolve %s.%s", pkg, name)
		}
		return t.Type1(), nil
	}
}

func SerializeStruct(v interface{}) ([]byte, error) {
	if v == nil {
		return jsoniter.Marshal(v)
	}
	return jsoniter.Marshal(StructDesc{
		Type: serializeTypeInfo(reflect.TypeOf(v)),
		Data: v,
	})
}

func DeserializeStruct(data []byte) (interface{}, error) {
	if bytes.Equal(data, []byte("null")) {
		return nil, nil
	}
	desc := new(struct {
		Type string              `json:"@type"`
		Data jsoniter.RawMessage `json:"data"`
	})
	if err := jsoniter.Unmarshal(data, desc); err != nil {
		return nil, errors.Wrap(err, "deserialize descriptor")
	}
	typ, err := deserializeType(desc.Type)
	if err != nil {
		return nil, err
	}

	v := reflect.New(typ).Interface()
	if err := jsoniter.Unmarshal(desc.Data, v); err != nil {
		return nil, errors.Wrapf(err, "deserialize struct data %s", string(desc.Data))
	}
	return reflect.ValueOf(v).Elem().Interface(), nil
}

func RegisterType(v interface{}) interface{} {
	reflect2.TypeOf(v)
	return nil
}
