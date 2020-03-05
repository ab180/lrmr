package output

import (
	"google.golang.org/grpc"
	"math"
	"time"
)

type Options struct {
	DialTimeout time.Duration
	DialOpts    []grpc.DialOption

	BufferLength   int
	MaxSendMsgSize int
}

func DefaultOptions() Options {
	return Options{
		DialTimeout: 5 * time.Second,
		DialOpts: []grpc.DialOption{
			grpc.WithInsecure(),
		},
		BufferLength:   1000,
		MaxSendMsgSize: math.MaxInt32,
	}
}
