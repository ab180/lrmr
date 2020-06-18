package serialization

import (
	"bytes"
	"reflect"

	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

type StructDesc struct {
	Type Type        `json:"@type"`
	Data interface{} `json:"data"`
}

func SerializeStruct(v interface{}) ([]byte, error) {
	if v == nil {
		return jsoniter.Marshal(v)
	}
	return jsoniter.Marshal(StructDesc{
		Type: TypeOf(v),
		Data: v,
	})
}

func DeserializeStruct(data []byte) (interface{}, error) {
	if bytes.Equal(data, []byte("null")) {
		return nil, nil
	}
	desc := new(struct {
		Type Type                `json:"@type"`
		Data jsoniter.RawMessage `json:"data"`
	})
	if err := jsoniter.Unmarshal(data, desc); err != nil {
		return nil, errors.Wrap(err, "deserialize descriptor")
	}

	v := desc.Type.New()
	if err := jsoniter.Unmarshal(desc.Data, v); err != nil {
		return nil, errors.Wrapf(err, "deserialize struct data %s", string(desc.Data))
	}
	return reflect.ValueOf(v).Elem().Interface(), nil
}
