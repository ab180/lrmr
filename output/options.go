package output

import (
	"github.com/creasty/defaults"
)

type Options struct {
	BufferLength int `default:"10000"`
}

func DefaultOptions() (o Options) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	return
}
