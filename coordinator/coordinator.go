package coordinator

import (
	"context"
	"errors"
)

var (
	ErrNotFound   = errors.New("key not found")
	ErrNotCounter = errors.New("key is not a counter")
)

type Coordinator interface {
	Get(ctx context.Context, key string, valuePtr interface{}) error
	Scan(ctx context.Context, prefix string) (results []RawItem, err error)
	Put(ctx context.Context, key string, value interface{}) error
	Batch(ctx context.Context, ops ...BatchOp) error

	// Watch subscribes modification events of the keys starting with given prefix.
	Watch(ctx context.Context, prefix string) chan WatchEvent

	// IncrementCounter is an atomic operation increasing the counter in given key.
	// returns a increased value of the counter right after the operation.
	IncrementCounter(ctx context.Context, key string) (count int64, err error)
	ReadCounter(ctx context.Context, key string) (count int64, err error)

	Delete(ctx context.Context, prefix string) (deleted int64, err error)
}

// Put returns a batch operation setting the value of a key.
func Put(key string, value interface{}) BatchOp {
	return BatchOp{
		Type:  PutEvent,
		Key:   key,
		Value: value,
	}
}

// IncrementCounter returns a batch operation incrementing counter of a key.
func IncrementCounter(key string) BatchOp {
	return BatchOp{
		Type: CounterEvent,
		Key:  key,
	}
}
