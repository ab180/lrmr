package output

import (
	"github.com/ab180/lrmr/lrdd"
)

type Output interface {
	Write(...*lrdd.Row) error
	Close() error
}

type Node interface {
	Host() string
}
