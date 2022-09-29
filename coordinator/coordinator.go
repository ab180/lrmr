package coordinator

import (
	"context"
	"errors"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrNotFound   = errors.New("key not found")
	ErrNotCounter = errors.New("key is not a counter")
)

type Coordinator interface {
	KV

	// WithOptions returns a child key-value store interface with given options applied.
	WithOptions(opts ...WriteOption) KV

	// GrantLease creates a lease (a time-to-live expiration attachable to other keys)
	GrantLease(ctx context.Context, ttl time.Duration) (clientv3.LeaseID, error)

	// KeepAlive tries to extend given lease's TTL until the context is cancelled or reaches deadline.
	// The returned channel will be closed when the KeepAlive is stopped.
	KeepAlive(ctx context.Context, lease clientv3.LeaseID) (<-chan struct{}, error)

	// Close closes coordinator.
	Close() error
}

type KV interface {
	Put(ctx context.Context, key string, value interface{}, opts ...WriteOption) error
	Get(ctx context.Context, key string, valuePtr interface{}) error
	Scan(ctx context.Context, prefix string) (results []RawItem, err error)

	// Delete remove all keys starting with given prefix.
	Delete(ctx context.Context, prefix string) (deleted int64, err error)

	// Watch subscribes modification events of the keys starting with given prefix.
	Watch(ctx context.Context, prefix string) chan WatchEvent

	// IncrementCounter is an atomic operation increasing the counter in given key.
	// returns a increased value of the counter right after the operation.
	IncrementCounter(ctx context.Context, key string) (count int64, err error)
	ReadCounter(ctx context.Context, key string) (count int64, err error)

	// CAS is an atomic compare-and-swap operation.
	// If the old is nil, it puts only if the key does not exist.
	// If the new is nil, it performs a delete operation instead of putting null to etcd. (In this case, opts is ignored)
	CAS(ctx context.Context, key string, old interface{}, new interface{}, opts ...WriteOption) (swapped bool, err error)

	// Commit apply changes of the transaction.
	// The transaction will be failed if one of the operation in the transaction fails.
	Commit(ctx context.Context, t *Txn, opts ...WriteOption) ([]TxnResult, error)
}

type WriteOption func(o *WriteOptions)

type WriteOptions struct {
	Lease clientv3.LeaseID
}

func WithLease(l clientv3.LeaseID) WriteOption {
	return func(o *WriteOptions) {
		o.Lease = l
	}
}

func buildWriteOption(opt []WriteOption) (o WriteOptions) {
	for _, optApplyFn := range opt {
		optApplyFn(&o)
	}
	return o
}
