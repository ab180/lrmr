package transformation

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
)

type Transformation interface {
	Apply(ctx Context, in chan *lrdd.Row, out output.Output) error
}
