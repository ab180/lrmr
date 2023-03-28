package retry

import (
	"errors"
	"fmt"
)

// DoWithResult do a given function with retry.
func DoWithResult[T any](fn func() (T, error), opts ...OptionFunc) (T, error) {
	opt := defaultOption()
	for _, o := range opts {
		o(&opt)
	}

	var retryCount int
	for {
		t, err := fn()
		if err != nil {
			retryCount++
			if retryCount >= opt.maxRetryCount {
				return t, errors.Join(err, fmt.Errorf("retry count exceeded: %d", retryCount))
			}

			continue
		}

		return t, nil
	}
}
