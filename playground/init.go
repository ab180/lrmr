package playground

import (
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/transformation"
)

var log = logger.New("playground")

func init() {
	transformation.Register(&ndjsonDecoder{})
	transformation.Register(&Counter{})
}
