package lrdd

import sync "sync"

type Bytes []byte

func NewBytes(bs string) *Bytes {
	bytes := Bytes(bs)
	return &bytes
}

func (bs *Bytes) MarshalMsg([]byte) ([]byte, error) {
	res := make([]byte, len(*bs))
	copy(res, *bs)
	return res, nil
}

func (bs *Bytes) UnmarshalMsg(in []byte) ([]byte, error) {
	(*bs) = make([]byte, len(in))
	copy(*bs, in)

	return nil, nil
}

func (bs *Bytes) Type() RowType {
	return RowTypeBytes
}

func (bs *Bytes) ReturnToPool() {
	(*bs) = (*bs)[:0]

	bytesPool.Put(bs)
}

func init() {
	RegisterValue(
		RowTypeBytes,
		func() MarshalUnmarshaler {
			return bytesPool.Get().(*Bytes)
		})
}

var bytesPool = sync.Pool{
	New: func() any {
		return &Bytes{}
	},
}
