package test

import (
	"encoding/binary"
	"fmt"

	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
)

var _ = lrmr.RegisterTypes(&Multiply{})

// Multiply multiplies input.
type Multiply struct{}

func (m *Multiply) Map(ctx lrmr.Context, rows []*lrdd.Row) ([]*lrdd.Row, error) {
	mappedRows := make([]*lrdd.Row, len(rows))
	for i, row := range rows {
		n := int32(*row.Value.(*int32Row))
		mappedRows[i] = &lrdd.Row{Value: newInt32Row(int32(n * 2))}
	}

	return mappedRows, nil
}

func (m *Multiply) RowID() lrdd.RowID {
	return rowIDInt32
}

func Map() *lrmr.Pipeline {
	const dataLen = 1000
	rows := make([]*lrdd.Row, dataLen)
	for i := 0; i < dataLen; i++ {
		rows[i] = &lrdd.Row{Value: newInt32Row(int32(i + 1))}
	}

	return lrmr.Parallelize(rows).
		Map(&Multiply{}).
		Map(&Multiply{}).
		Map(&Multiply{})
}

type int32Row int32

func newInt32Row(i int32) *int32Row {
	u := int32Row(i)
	return &u
}

func (i *int32Row) MarshalMsg([]byte) ([]byte, error) {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(*i))

	return bs, nil
}

func (i *int32Row) UnmarshalMsg(in []byte) ([]byte, error) {
	*i = int32Row(binary.LittleEndian.Uint32(in))

	return nil, nil
}

func (i *int32Row) ID() lrdd.RowID {
	return rowIDInt32
}

func (i *int32Row) String() string {
	return fmt.Sprintf("%d", *i)
}

func init() {
	lrdd.RegisterValue(
		rowIDInt32,
		func() lrdd.MarshalUnmarshaler {
			var v int32Row
			return &v
		})
}

const rowIDInt32 lrdd.RowID = 3
