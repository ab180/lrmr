package lrdd

import "fmt"

type Row struct {
	Key   string
	Value MarshalUnmarshaler
}

type MarshalUnmarshaler interface {
	MarshalMsg([]byte) ([]byte, error)
	UnmarshalMsg([]byte) ([]byte, error)
	Type() RowType
	String() string
}

type RowType int32

const (
	RowTypeBytes  RowType = 1
	RowTypeUint64 RowType = 2
)

func GetValue(rowType RowType) MarshalUnmarshaler {
	switch rowType {
	case RowTypeBytes:
		return &Bytes{}
	case RowTypeUint64:
		i := Uint64(0)
		return &i
	default:
		return rowTypes[rowType]()
	}
}

func RegisterValue(rowType RowType, newFunc func() MarshalUnmarshaler) {
	switch rowType {
	case RowTypeBytes:
	case RowTypeUint64:
		panic("cannot register builtin row type")
	default:
		_, ok := rowTypes[rowType]
		if ok {
			panic(fmt.Sprintf("row type %v already registered", rowType))
		}

		rowTypes[rowType] = newFunc
	}

}

var rowTypes = map[RowType]func() MarshalUnmarshaler{}
