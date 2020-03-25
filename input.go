package lrmr

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"os"
	"path/filepath"
)

type InputProvider interface {
	ProvideInput(out output.Output) error
}

type localInput struct {
	Path string
}

func (l localInput) ProvideInput(out output.Output) error {
	return filepath.Walk(l.Path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		return out.Write([]*lrdd.Row{lrdd.Value(path)})
	})
}
