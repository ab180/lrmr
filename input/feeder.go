package input

import (
	"github.com/ab180/lrmr/output"
)

type Feeder interface {
	FeedInput(out output.Output) error
}
