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

	// ReturnToPool returns the value to the pool.
	//
	// If you do not want use the pool, you can just empty the method.
	ReturnToPool()
}

type RowType int32

const (
	RowTypeBytes  RowType = 1
	RowTypeUint64 RowType = 2
)

func GetValue(rowType RowType) MarshalUnmarshaler {
	return rowTypes[rowType]()
}

// RegisterValue registers a new value type.
//
// If you want pooling the value, you should implement the MarshalUnmarshaler.ReturnToPool method.
func RegisterValue(rowType RowType, newFunc func() MarshalUnmarshaler) {
	_, ok := rowTypes[rowType]
	if ok {
		panic(fmt.Sprintf("row type %v already registered", rowType))
	}

	rowTypes[rowType] = newFunc
}

var rowTypes = map[RowType]func() MarshalUnmarshaler{}
