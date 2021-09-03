package lrmrmetric

import (
	"sync"

	"go.uber.org/atomic"
)

type Repository interface {
	AddMetric(name string, delta int64)
	SetMetric(name string, val int64)
	Collect() Metrics
}

type repository struct {
	metrics sync.Map
}

func NewRepository() Repository {
	return &repository{}
}

func (r *repository) AddMetric(name string, delta int64) {
	existVal, loaded := r.metrics.LoadOrStore(name, atomic.NewInt64(delta))
	if loaded {
		existVal.(*atomic.Int64).Add(delta)
	}
}

func (r *repository) SetMetric(name string, val int64) {
	r.metrics.Store(name, atomic.NewInt64(val))
	existVal, loaded := r.metrics.LoadOrStore(name, atomic.NewInt64(val))
	if loaded {
		existVal.(*atomic.Int64).Store(val)
	}
}

func (r *repository) Collect() Metrics {
	metrics := make(Metrics)
	r.metrics.Range(func(key, value interface{}) bool {
		metrics[key.(string)] = uint64(value.(*atomic.Int64).Load())
		return true
	})
	return metrics
}
