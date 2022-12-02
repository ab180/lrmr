package lrdd

import (
	"encoding/binary"
	"fmt"
	"time"
	"unsafe"
)

const durationSize = unsafe.Sizeof(time.Duration(0)) // #nosec G103

// FromStrings converts string values to a Row slice.
func FromStrings(vals ...string) []*Row {
	rows := make([]*Row, len(vals))
	for i, val := range vals {
		rows[i] = &Row{Value: NewBytes(val)}
	}

	return rows
}

// FromStringMap converts a map of string values to a Row slice.
func FromStringMap(vals map[string]string) []*Row {
	var rows []*Row
	for key, val := range vals {
		rows = append(rows, &Row{Key: key, Value: NewBytes(val)})
	}

	return rows
}

// FromStringSliceMap converts a map of string slices to a Row slice.
func FromStringSliceMap(vals map[string][]string) []*Row {
	var rows []*Row
	for key, slice := range vals {
		for _, val := range slice {
			rows = append(rows, &Row{Key: key, Value: NewBytes(val)})
		}
	}

	return rows
}

// FromInts converts int values to a Row slice.
func FromInts(vals ...int) []*Row {
	rows := make([]*Row, len(vals))
	for i, val := range vals {
		intVal := Uint64(val)
		rows[i] = &Row{Value: &intVal}
	}

	return rows
}

// FromIntSliceMap converts a map of int slices to a Row slice.
func FromIntSliceMap(vals map[string][]int) []*Row {
	var rows []*Row
	for key, slice := range vals {
		for _, val := range slice {
			rows = append(rows, &Row{Key: key, Value: NewBytes(fmt.Sprintf("%d", val))})
		}
	}

	return rows
}

// FromDurations converts duration values to a Row slice.
func FromDurations(vals ...time.Duration) []*RawRow {
	rows := make([]*RawRow, len(vals))
	for i, val := range vals {
		rows[i] = &RawRow{
			Value: make([]byte, durationSize),
		}
		binary.LittleEndian.PutUint64(rows[i].Value, uint64(val))
	}
	return rows
}

// ToDuration converts a Row to a duration.
func ToDuration(row *RawRow) time.Duration {
	return time.Duration(binary.LittleEndian.Uint64(row.Value))
}
