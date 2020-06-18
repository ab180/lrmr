package lrdd

import (
	"reflect"

	"github.com/therne/lrmr/lrmrpb"
	"github.com/vmihailenco/msgpack/v5"
)

type Row struct {
	Key string

	localValue interface{}
	valueData  []byte
}

func FromProto(row *lrmrpb.Row) *Row {
	return &Row{
		Key:       row.Key,
		valueData: row.Value,
	}
}

func (m Row) UnmarshalValue(ptr interface{}) {
	if m.localValue != nil {
		// Row is being transferred on local
		reflect.ValueOf(ptr).Elem().Set(reflect.ValueOf(m.localValue))
		return
	}
	err := msgpack.Unmarshal(m.valueData, ptr)
	if err != nil {
		panic(err)
	}
}

func (m Row) ToProto() *lrmrpb.Row {
	raw, err := msgpack.Marshal(m.localValue)
	if err != nil {
		panic(err)
	}
	return &lrmrpb.Row{
		Key:   m.Key,
		Value: raw,
	}
}

func Value(v interface{}) *Row {
	return &Row{localValue: v}
}

func KeyValue(k string, v interface{}) *Row {
	return &Row{Key: k, localValue: v}
}
