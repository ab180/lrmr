package lrdd

import sync "sync"

func GetRawRow() *RawRow {
	return pool.Get().(*RawRow)
}

func PutRawRow(row *RawRow) {
	value := row.Value[:0]
	row.Reset()
	row.Value = value
	pool.Put(row)
}

var pool = sync.Pool{
	New: func() any {
		return &RawRow{}
	},
}
