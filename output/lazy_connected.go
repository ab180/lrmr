package output

import (
	"github.com/ab180/lrmr/lrdd"
	"github.com/pkg/errors"
)

type lazyInit struct {
	initFn Initializer
	output Output
}

type Initializer func() (Output, error)

// LazyInitialized wraps an Output. It initializes Output as first data written on the output.
// It can be effectively used when destination is not ready at the moment (e.g. PushData)
func LazyInitialized(initFn Initializer) Output {
	return &lazyInit{initFn: initFn}
}

func (l *lazyInit) Write(row []lrdd.Row) error {
	if l.output == nil {
		out, err := l.initFn()
		if err != nil {
			return errors.Wrap(err, "initialize output")
		}
		l.output = out
	}
	return l.output.Write(row)
}

func (l *lazyInit) Close() error {
	if l.output == nil {
		return nil
	}
	return l.output.Close()
}
