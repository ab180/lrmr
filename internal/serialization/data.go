package serialization

import (
	"encoding/json"

	"github.com/vmihailenco/msgpack/v5"
)

func SerializeData(v interface{}) ([]byte, error) {
	if jm, ok := v.(json.Marshaler); ok {
		return jm.MarshalJSON()
	}
	return msgpack.Marshal(v)
}

func DeserializeData(data []byte, ptrToV interface{}) error {
	if ju, ok := ptrToV.(json.Unmarshaler); ok {
		return ju.UnmarshalJSON(data)
	}
	return msgpack.Unmarshal(data, ptrToV)
}
