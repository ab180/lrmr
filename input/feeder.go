package input

import (
	"github.com/ab180/lrmr/output"
)

const FeederPartitionID = "__input"

type Feeder interface {
	FeedInput(out output.Output) error
}
