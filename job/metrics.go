package job

type Metrics map[string]int

// Sum merges two metrics. When a key collides, it sums two key.
func (m Metrics) Sum(o Metrics) (merged Metrics) {
	merged = make(Metrics)
	for k, v := range m {
		merged[k] += v
	}
	for k, v := range o {
		merged[k] += v
	}
	return
}

// Assign merges two metrics. When a key collides,
// it overrides the key with one's from given metric.
func (m Metrics) Assign(o Metrics) (merged Metrics) {
	merged = make(Metrics)
	for k, v := range m {
		merged[k] = v
	}
	for k, v := range o {
		merged[k] = v
	}
	return
}

// AddPrefix returns new metric where all keys prefixed with given prefix.
func (m Metrics) AddPrefix(p string) (prefixed Metrics) {
	prefixed = make(Metrics)
	for k, v := range m {
		prefixed[p+k] = v
	}
	return
}
