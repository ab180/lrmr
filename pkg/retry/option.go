package retry

import "time"

type option struct {
	maxRetryCount int
	delay         time.Duration
}

func defaultOption() option {
	return option{
		maxRetryCount: 3,
		delay:         200 * time.Millisecond,
	}
}

// OptionFunc is a function that sets an option.
type OptionFunc func(*option)

// WithRetryCount sets the maximum number of retries.
func WithRetryCount(count int) OptionFunc {
	return func(o *option) {
		o.maxRetryCount = count
	}
}

// WithDelay sets the delay between retries.
func WithDelay(delay time.Duration) OptionFunc {
	return func(o *option) {
		o.delay = delay
	}
}
