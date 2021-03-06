package lrmr

import (
	"os"
	"path/filepath"

	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/output"
	"github.com/ab180/lrmr/partitions"
)

type InputProvider interface {
	partitions.Partitioner
	FeedInput(out output.Output) error
}

type localInput struct {
	partitions.ShuffledPartitioner
	Path string
}

func (l localInput) FeedInput(out output.Output) error {
	return filepath.Walk(l.Path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		return out.Write(lrdd.Value(path))
	})
}

type parallelizedInput struct {
	partitions.ShuffledPartitioner
	data []*lrdd.Row
}

func (p parallelizedInput) FeedInput(out output.Output) error {
	return out.Write(p.data...)
}
