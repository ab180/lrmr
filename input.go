package lrmr

import (
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
	"os"
	"path/filepath"
)

type InputProvider interface {
	ProvideInput(out output.Output) error
}

type inputProviderWrapper struct {
	transformation.Simple
	provider InputProvider
}

var _ = transformation.Register(&inputProviderWrapper{})

func (i inputProviderWrapper) Apply(row lrdd.Row, out output.Output, executorID int) error {
	return i.provider.ProvideInput(out)
}

type localInput struct {
	transformation.Simple
	Path string
}

var _ = transformation.Register(&localInput{})

func (l localInput) Apply(row lrdd.Row, out output.Output, executorID int) error {
	return filepath.Walk(l.Path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		return out.Send(lrdd.Row{"path": path})
	})
}
