package lrdd

import (
	"encoding/binary"
	sync "sync"
)

type Uint64 uint64

func NewUint64(i uint64) *Uint64 {
	u := Uint64(i)
	return &u
}

func (i *Uint64) MarshalMsg([]byte) ([]byte, error) {
	bs := make([]byte, 8)
	binary.LittleEndian.PutUint64(bs, uint64(*i))

	return bs, nil
}

func (i *Uint64) UnmarshalMsg(in []byte) ([]byte, error) {
	*i = Uint64(binary.LittleEndian.Uint64(in))

	return nil, nil
}

func (i *Uint64) Type() RowType {
	return RowTypeUint64
}

func (i *Uint64) ReturnToPool() {
	*i = 0

	uint64Pool.Put(i)
}

func init() {
	RegisterValue(
		RowTypeUint64,
		func() MarshalUnmarshaler {
			return uint64Pool.Get().(*Uint64)
		})
}

var uint64Pool = sync.Pool{
	New: func() any {
		return NewUint64(0)
	},
}
