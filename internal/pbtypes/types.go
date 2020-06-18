package pbtypes

import (
	jsoniter "github.com/json-iterator/go"
)

func MustMarshalJSON(v interface{}) *JSON {
	raw, err := jsoniter.Marshal(v)
	if err != nil {
		panic(err)
	}
	return &JSON{Json: raw}
}

// UnmarshalJSON deserializes the raw JSON to the given pointer.
// noinspection GoStandardMethods
func (m *JSON) UnmarshalJSON(ptrToVal interface{}) error {
	return jsoniter.Unmarshal(m.Json, ptrToVal)
}
