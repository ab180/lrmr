package lrmrmetric

import (
	"fmt"
	"sort"

	"github.com/thoas/go-funk"
)

type Metrics map[string]uint64

// Add merges two metrics. When a key collides, it sums two key.
func (m Metrics) Add(o Metrics) {
	for k, v := range o {
		m[k] += v
	}
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

func (m Metrics) String() string {
	keys := funk.Keys(m).([]string)
	sort.Strings(keys)

	metricLogs := ""
	for _, key := range keys {
		metricLogs += fmt.Sprintf(" - %s: %d\n", key, m[key])
	}
	return metricLogs
}
