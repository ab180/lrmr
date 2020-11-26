package worker

import (
	"github.com/ab180/lrmr/input"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
)

type LocalPipe struct {
	reader *input.Reader
}

func NewLocalPipe(r *input.Reader) *LocalPipe {
	l := &LocalPipe{reader: r}
	r.Add(l)
	return l
}

func (l *LocalPipe) CloseWithStatus(s job.Status) error {
	return nil
}

func (l *LocalPipe) Write(rows ...*lrdd.Row) error {
	l.reader.C <- rows
	return nil
}

func (l *LocalPipe) Close() error {
	l.reader.Done()
	l.reader = nil
	return nil
}
