package lrdd

import "fmt"

type Row struct {
	Key   string             `json:"key,omitempty"`
	Value MarshalUnmarshaler `json:"value,omitempty"`
}

type MarshalUnmarshaler interface {
	MarshalMsg([]byte) ([]byte, error)
	UnmarshalMsg([]byte) ([]byte, error)
	Type() RowType
}

type RowType int32

const (
	RowTypeBytes  RowType = 1
	RowTypeUint64 RowType = 2
)

func GetValue(rowType RowType) MarshalUnmarshaler {
	return rowTypes[rowType]()
}

func RegisterValue(rowType RowType, newFunc func() MarshalUnmarshaler) {
	_, ok := rowTypes[rowType]
	if ok {
		panic(fmt.Sprintf("row type %v already registered", rowType))
	}

	rowTypes[rowType] = newFunc
}

var rowTypes = map[RowType]func() MarshalUnmarshaler{}
