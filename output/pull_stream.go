package output

import (
	"github.com/ab180/lrmr/lrdd"
)

type PullStream struct {
	queue chan *lrdd.Row
}

func NewPullStream(size int) PullStream {
	return PullStream{
		queue: make(chan *lrdd.Row, size),
	}
}

func (p PullStream) Write(row ...*lrdd.Row) error {
	for _, r := range row {
		p.queue <- r
	}
	return nil
}

func (p PullStream) Dispatch(n int) []*lrdd.Row {
	rows := make([]*lrdd.Row, n)
	for i := 0; i < n; i++ {
		rows[i] = <-p.queue
	}
	return rows
}

func (p PullStream) Close() error {
	close(p.queue)
	return nil
}

// PullStream implements output.Output interface.
var _ Output = (*PullStream)(nil)
