package retry

type option struct {
	maxRetryCount int
}

func defaultOption() option {
	return option{
		maxRetryCount: 3,
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
