package lrdd

import (
	"encoding/binary"
	"fmt"
	"time"
	"unsafe"
)

const durationSize = unsafe.Sizeof(time.Duration(0))

// FromStrings converts string values to a Row slice.
func FromStrings(vals ...string) []*Row {
	rows := make([]*Row, len(vals))
	for i, val := range vals {
		rows[i] = &Row{Value: []byte(val)}
	}

	return rows
}

// FromStringMap converts a map of string values to a Row slice.
func FromStringMap(vals map[string]string) []*Row {
	var rows []*Row
	for key, val := range vals {
		rows = append(rows, &Row{Key: key, Value: []byte(val)})
	}

	return rows
}

// FromStringSliceMap converts a map of string slices to a Row slice.
func FromStringSliceMap(vals map[string][]string) []*Row {
	var rows []*Row
	for key, slice := range vals {
		for _, val := range slice {
			rows = append(rows, &Row{Key: key, Value: []byte(val)})
		}
	}

	return rows
}

// FromInts converts int values to a Row slice.
func FromInts(vals ...int) []*Row {
	rows := make([]*Row, len(vals))
	for i, val := range vals {
		rows[i] = &Row{Value: []byte(fmt.Sprintf("%d", val))}
	}

	return rows
}

// FromIntSliceMap converts a map of int slices to a Row slice.
func FromIntSliceMap(vals map[string][]int) []*Row {
	var rows []*Row
	for key, slice := range vals {
		for _, val := range slice {
			rows = append(rows, &Row{Key: key, Value: []byte(fmt.Sprintf("%d", val))})
		}
	}

	return rows
}

// FromDurations converts duration values to a Row slice.
func FromDurations(vals ...time.Duration) []*Row {
	rows := make([]*Row, len(vals))
	for i, val := range vals {
		rows[i] = &Row{
			Value: make([]byte, durationSize),
		}
		binary.LittleEndian.PutUint64(rows[i].Value, uint64(val))
	}
	return rows
}

// ToDuration converts a Row to a duration.
func ToDuration(row *Row) time.Duration {
	return time.Duration(binary.LittleEndian.Uint64(row.Value))
}
