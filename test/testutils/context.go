package testutils

import (
	"context"
	"time"

	"github.com/smartystreets/goconvey/convey"
)

const defaultTimeout = 5 * time.Second

// ContextWithTimeout returns a context.Context which has a timeout and usable on convey suite.
// the timeout is defaults to 5 seconds, but you can override it with optional argument.
func ContextWithTimeout(overrideTimeout ...time.Duration) context.Context {
	timeout := defaultTimeout
	if len(overrideTimeout) > 0 {
		timeout = overrideTimeout[0]
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	convey.Reset(func() {
		cancel()
	})
	return ctx
}
