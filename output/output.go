package output

import (
	"github.com/ab180/lrmr/lrdd"
	"github.com/airbloc/logger"
)

var log = logger.New("output")

type Output interface {
	Write(...*lrdd.Row) error
	Close() error
}

type Node interface {
	Host() string
}
