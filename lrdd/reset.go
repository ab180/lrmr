package lrdd

func (row *RawRow) RemainCapicityReset() {
	value := row.Value[:0]
	row.Reset()
	row.Value = value
}
