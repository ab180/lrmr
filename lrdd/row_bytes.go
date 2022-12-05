package lrdd

type Bytes []byte

func NewBytes(bs string) *Bytes {
	bytes := Bytes(bs)
	return &bytes
}

func (bs *Bytes) MarshalMsg([]byte) ([]byte, error) {
	return *bs, nil
}

func (bs *Bytes) UnmarshalMsg(in []byte) ([]byte, error) {
	(*bs) = in

	return nil, nil
}

func (bs *Bytes) Type() RowType {
	return RowTypeBytes
}

func (bs *Bytes) String() string {
	return string(*bs)
}

func init() {
	RegisterValue(
		RowTypeBytes,
		func() MarshalUnmarshaler {
			return &Bytes{}
		})
}
