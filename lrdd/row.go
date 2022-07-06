package lrdd

import (
	"github.com/vmihailenco/msgpack/v5"
)

func (m *Row) UnmarshalValue(ptr interface{}) {
	err := msgpack.Unmarshal(m.Value, ptr)
	if err != nil {
		panic(err)
	}
}

func Value(v interface{}) *Row {
	return &Row{Value: mustEncode(v)}
}

func KeyValue(k string, v interface{}) *Row {
	return &Row{Key: k, Value: mustEncode(v)}
}

func mustEncode(v interface{}) []byte {
	raw, err := msgpack.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}
