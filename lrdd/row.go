package lrdd

import (
	"github.com/shamaton/msgpack"
)

func (m Row) UnmarshalValue(ptr interface{}) {
	err := msgpack.Decode(m.Value, ptr)
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
	raw, err := msgpack.Encode(v)
	if err != nil {
		panic(err)
	}
	return raw
}
