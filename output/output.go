package output

import (
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/lrdd"
)

var log = logger.New("output")

type Output interface {
	Write([]*lrdd.Row) error
	Close() error
}

type Node interface {
	Host() string
	NodeID() string
}
