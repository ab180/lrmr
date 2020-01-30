package dataframe

import "github.com/vmihailenco/msgpack"

const (
	metadataKey = "__metadata"
)

type Row map[string]interface{}

func (r *Row) Unmarshal(data []byte) error {
	return msgpack.Unmarshal(data, r)
}

func (r *Row) Marshal() []byte {
	b, err := msgpack.Marshal(r)
	if err != nil {
		panic(err)
	}
	return b
}

func (r Row) Merge(a Row) Row {
	o := make(Row)
	for k := range r {
		o[k] = r[k]
	}
	for k := range a {
		o[k] = a[k]
	}
	return o
}
