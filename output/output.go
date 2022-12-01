package output

import (
	"github.com/ab180/lrmr/lrdd"
)

type Output interface {
	// Write writes rows to the output.
	// The elements of the slice is call-clobbered, so the caller should not
	// reuse them after the call.
	// But the slice itself can be reused, so the callee should not retain it.
	Write([]*lrdd.Row) error
	Close() error
}

type Node interface {
	Host() string
}
