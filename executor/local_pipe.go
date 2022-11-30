package executor

import (
	"github.com/ab180/lrmr/input"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
)

type LocalPipe struct {
	nextStageReader *input.Reader
}

func NewLocalPipe(r *input.Reader) *LocalPipe {
	r.Add()
	return &LocalPipe{nextStageReader: r}
}

func (l *LocalPipe) CloseWithStatus(s job.Status) error {
	return nil
}

func (l *LocalPipe) Write(rows []*lrdd.Row) error {
	l.nextStageReader.Write(rows)
	return nil
}

func (l *LocalPipe) Close() error {
	l.nextStageReader.Done()
	l.nextStageReader = nil
	return nil
}
