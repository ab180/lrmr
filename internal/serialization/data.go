package serialization

import (
	"encoding/json"
	"github.com/shamaton/msgpack"
)

func SerializeData(v interface{}) ([]byte, error) {
	if jm, ok := v.(json.Marshaler); ok {
		return jm.MarshalJSON()
	}
	return msgpack.Encode(v)
}

func DeserializeData(data []byte, ptrToV interface{}) error {
	if ju, ok := ptrToV.(json.Unmarshaler); ok {
		return ju.UnmarshalJSON(data)
	}
	return msgpack.Decode(data, ptrToV)
}
