package lrdd

import "github.com/shamaton/msgpack"

type Row struct {
	Key   string
	Value []byte
}

func (r Row) Encode() []byte {
	return mustEncode(r)
}

func (r Row) UnmarshalValue(ptr interface{}) {
	err := msgpack.Decode(r.Value, ptr)
	if err != nil {
		panic(err)
	}
}

func Value(v interface{}) Row {
	return Row{Value: mustEncode(v)}
}

func KeyValue(k string, v interface{}) Row {
	return Row{Key: k, Value: mustEncode(v)}
}

func Decode(data []byte) (r Row, err error) {
	err = msgpack.Decode(data, &r)
	return
}

func mustEncode(v interface{}) []byte {
	raw, err := msgpack.Encode(v)
	if err != nil {
		panic(err)
	}
	return raw
}
