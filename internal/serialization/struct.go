package serialization

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/modern-go/reflect2"
	"github.com/pkg/errors"
	"reflect"
)

// ErrUnresolved is returned when the type with given package path and name does not exist.
// It usually caused by unuse; Go compiler erases unused and unimported types, so you need to ensure that
// receiver of the serialized struct imports the referred type.
var ErrUnresolved = errors.New("unresolved type")

type StructDesc struct {
	PkgPath string      `json:"pkgPath"`
	Name    string      `json:"name"`
	Data    interface{} `json:"data"`
}

func SerializeStruct(v interface{}) ([]byte, error) {
	typ := reflect.TypeOf(v)
	return jsoniter.Marshal(StructDesc{
		PkgPath: typ.PkgPath(),
		Name:    typ.Name(),
		Data:    v,
	})
}

func DeserializeStruct(data []byte) (interface{}, error) {
	desc := new(struct {
		PkgPath string              `json:"pkgPath"`
		Name    string              `json:"name"`
		Data    jsoniter.RawMessage `json:"data"`
	})
	if err := jsoniter.Unmarshal(data, desc); err != nil {
		return nil, errors.Wrap(err, "deserialize descriptor")
	}
	typ := reflect2.TypeByPackageName(desc.PkgPath, desc.Name)
	if typ == nil {
		return nil, errors.Wrapf(ErrUnresolved, "resolve %s.(%s)", desc.PkgPath, desc.Name)
	}
	v := typ.New()
	if err := jsoniter.Unmarshal(desc.Data, v); err != nil {
		return nil, errors.Wrapf(err, "deserialize struct data %s", string(desc.Data))
	}
	return v, nil
}
