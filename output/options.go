package output

import (
	"math"
)

type Options struct {
	BufferLength   int
	MaxSendMsgSize int
}

func DefaultOptions() Options {
	return Options{
		BufferLength:   1000,
		MaxSendMsgSize: math.MaxInt32,
	}
}
