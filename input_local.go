package lrmr

import (
	"github.com/therne/lrmr/dataframe"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
	"os"
	"path/filepath"
)

type LocalInput struct {
	transformation.Simple
	Path string
}

func NewLocalInput(path string) transformation.Transformation {
	return &LocalInput{
		Path: path,
	}
}

func (l *LocalInput) DescribeOutput() *transformation.OutputDesc {
	return transformation.DescribingOutput().WithRoundRobin()
}

func (l *LocalInput) Run(row dataframe.Row, out output.Output) error {
	return filepath.Walk(l.Path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		msg := make(dataframe.Row)
		msg["path"] = path
		return out.Send(msg)
	})
}
