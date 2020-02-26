package lrdd

import (
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
	"os"
	"path/filepath"
)

type input struct {
	transformation.Simple
	Path string
}

func newInput(path string) transformation.Transformation {
	return &input{
		Path: path,
	}
}

func (l *input) DescribeOutput() *transformation.OutputDesc {
	return transformation.DescribingOutput().WithRoundRobin()
}

func (l *input) Run(row Row, out output.Output) error {
	return filepath.Walk(l.Path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		return out.Send(Row{"path": path})
	})
}
