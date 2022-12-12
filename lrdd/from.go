package lrdd

import (
	"encoding/binary"
	"fmt"
	"time"
	"unsafe"
)

const durationSize = unsafe.Sizeof(time.Duration(0)) // #nosec G103

// FromStrings converts string values to a Row slice.
func FromStrings(vals ...string) []Row {
	rows := make([]Row, len(vals))
	for i, val := range vals {
		rows[i] = Row{Value: NewBytes(val)}
	}

	return rows
}

// FromStringMap converts a map of string values to a Row slice.
func FromStringMap(vals map[string]string) []Row {
	var rows []Row
	for key, val := range vals {
		rows = append(rows, Row{Key: key, Value: NewBytes(val)})
	}

	return rows
}

// FromStringSliceMap converts a map of string slices to a Row slice.
func FromStringSliceMap(vals map[string][]string) []Row {
	var rows []Row
	for key, slice := range vals {
		for _, val := range slice {
			rows = append(rows, Row{Key: key, Value: NewBytes(val)})
		}
	}

	return rows
}

// FromInts converts int values to a Row slice.
func FromInts(vals ...int) []Row {
	rows := make([]Row, len(vals))
	for i, val := range vals {
		intVal := Uint64(val)
		rows[i] = Row{Value: &intVal}
	}

	return rows
}

// FromIntSliceMap converts a map of int slices to a Row slice.
func FromIntSliceMap(vals map[string][]int) []Row {
	var rows []Row
	for key, slice := range vals {
		for _, val := range slice {
			rows = append(rows, Row{Key: key, Value: NewBytes(fmt.Sprintf("%d", val))})
		}
	}

	return rows
}

// FromDurations converts duration values to a Row slice.
func FromDurations(vals ...time.Duration) []Row {
	rows := make([]Row, len(vals))
	for i, val := range vals {
		bs := make([]byte, durationSize)
		binary.LittleEndian.PutUint64(bs, uint64(val))
		lrddBs := Bytes(bs)

		rows[i] = Row{
			Value: &lrddBs,
		}
	}
	return rows
}

// ToDuration converts a Row to a duration.
func ToDuration(row Row) time.Duration {
	bs := row.Value.(*Bytes)

	return time.Duration(binary.LittleEndian.Uint64([]byte(*bs)))
}
