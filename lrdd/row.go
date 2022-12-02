package lrdd

import "fmt"

type Row struct {
	Key   string
	Value MarshalUnmarshaler
}

type MarshalUnmarshaler interface {
	MarshalMsg([]byte) ([]byte, error)
	UnmarshalMsg([]byte) ([]byte, error)
	ID() RowID
	String() string
}

type RowID int32

const (
	RowIDBytes  RowID = 1
	RowIDUint64 RowID = 2
)

func GetValue(rowID RowID) MarshalUnmarshaler {
	switch rowID {
	case RowIDBytes:
		return &Bytes{}
	case RowIDUint64:
		i := Uint64(0)
		return &i
	default:
		return rowTypes[rowID]()
	}
}

func RegisterValue(rowID RowID, newFunc func() MarshalUnmarshaler) {
	switch rowID {
	case RowIDBytes:
	case RowIDUint64:
		panic("cannot register builtin row type")
	default:
		_, ok := rowTypes[rowID]
		if ok {
			panic(fmt.Sprintf("row type %v already registered", rowID))
		}

		rowTypes[rowID] = newFunc
	}

}

var rowTypes = map[RowID]func() MarshalUnmarshaler{}
