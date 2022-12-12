package lrdd

import sync "sync"

func GetRawRow() *RawRow {
	return rawRowPool.Get().(*RawRow)
}

func PutRawRow(row *RawRow) {
	value := row.Value[:0]
	row.Reset()
	row.Value = value

	rawRowPool.Put(row)
}

func GetRows(size int) *[]Row {
	rows := rowsPool.Get().(*[]Row)
	if size <= cap(*rows) {
		*rows = (*rows)[:size]
	} else {
		*rows = make([]Row, size)
	}

	return rows
}

func PutRows(rows *[]Row) {
	for _, row := range *rows {
		row.Key = row.Key[:0]
		row.Value = nil
	}
	*rows = (*rows)[:0]

	rowsPool.Put(rows)
}

var (
	rawRowPool = sync.Pool{
		New: func() any {
			return &RawRow{}
		},
	}
	rowsPool = sync.Pool{
		New: func() any {
			return &[]Row{}
		},
	}
)
