package coordinator

import (
	"context"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/coreos/etcd/clientv3"
	jsoniter "github.com/json-iterator/go"
)

type localMemoryCoordinator struct {
	opt    localMemoryOptions
	data   sync.Map
	leases sync.Map

	counter     map[string]int64
	counterLock sync.RWMutex

	subscriptions []subscription
	subsLock      sync.RWMutex
}

type subscription struct {
	prefix string
	events chan WatchEvent
}

// NewLocalMemory creates local variable based coordinator.
// Only used for test purpose.
func NewLocalMemory(opts ...LocalMemoryOption) Coordinator {
	return &localMemoryCoordinator{
		counter: map[string]int64{},
	}
}

func (lmc *localMemoryCoordinator) simulate(ctx context.Context) error {
	time.Sleep(lmc.opt.simulatedDelay)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return lmc.opt.simulatedError
}

func (lmc *localMemoryCoordinator) Get(ctx context.Context, key string, valuePtr interface{}) error {
	if err := lmc.simulate(ctx); err != nil {
		return err
	}
	val, ok := lmc.data.Load(key)
	if !ok {
		return ErrNotFound
	}
	return val.(RawItem).Unmarshal(valuePtr)
}

func (lmc *localMemoryCoordinator) Scan(ctx context.Context, prefix string) (results []RawItem, err error) {
	if err := lmc.simulate(ctx); err != nil {
		return nil, err
	}
	lmc.data.Range(func(key, value interface{}) bool {
		if strings.HasPrefix(key.(string), prefix) {
			results = append(results, value.(RawItem))
		}
		return true
	})
	return
}

func (lmc *localMemoryCoordinator) Put(ctx context.Context, key string, value interface{}, opts ...clientv3.OpOption) error {
	if err := lmc.simulate(ctx); err != nil {
		return err
	}
	op := &clientv3.Op{}
	for _, opt := range opts {
		opt(op)
	}

	// trick for getting internal field
	leaseField := reflect.ValueOf(op).Elem().FieldByName("leaseID")
	leaseID := reflect.NewAt(reflect.TypeOf(clientv3.NoLease), unsafe.Pointer(leaseField.UnsafeAddr())).
		Elem().Interface().(clientv3.LeaseID)

	if v, ok := lmc.leases.Load(leaseID); leaseID != clientv3.NoLease && ok {
		ttl := v.(time.Duration)
		go func() {
			time.Sleep(ttl)
			_, _ = lmc.Delete(context.Background(), key)
			lmc.leases.Delete(leaseID)
		}()
	}
	return lmc.put(key, value)
}

func (lmc *localMemoryCoordinator) put(k string, v interface{}) error {
	raw, err := jsoniter.Marshal(v)
	if err != nil {
		return err
	}
	item := RawItem{
		Key:   k,
		Value: raw,
	}
	lmc.data.Store(k, item)
	go lmc.notifySubscribers(WatchEvent{
		Type: PutEvent,
		Item: item,
	})
	return nil
}

func (lmc *localMemoryCoordinator) IncrementCounter(ctx context.Context, key string) (count int64, err error) {
	if err = lmc.simulate(ctx); err != nil {
		return
	}
	count = lmc.incrementCounter(key)
	return
}

func (lmc *localMemoryCoordinator) incrementCounter(key string) (count int64) {
	lmc.data.Store(key, counterMark)

	lmc.counterLock.Lock()
	lmc.counter[key] += 1
	count = lmc.counter[key]
	lmc.counterLock.Unlock()

	go lmc.notifySubscribers(WatchEvent{
		Type:    CounterEvent,
		Item:    RawItem{Key: key},
		Counter: count,
	})
	return count
}

func (lmc *localMemoryCoordinator) ReadCounter(ctx context.Context, key string) (count int64, err error) {
	if err := lmc.simulate(ctx); err != nil {
		return 0, err
	}
	lmc.counterLock.RLock()
	defer lmc.counterLock.RUnlock()
	return lmc.counter[key], nil
}

func (lmc *localMemoryCoordinator) Commit(ctx context.Context, txn *Txn) error {
	if err := lmc.simulate(ctx); err != nil {
		return err
	}
	for _, op := range txn.Ops {
		switch op.Type {
		case PutEvent:
			if err := lmc.put(op.Key, op.Value); err != nil {
				return err
			}
		case CounterEvent:
			lmc.incrementCounter(op.Key)
		case DeleteEvent:
			lmc.delete(op.Key)
		}
	}
	return nil
}

func (lmc *localMemoryCoordinator) Delete(ctx context.Context, prefix string) (deleted int64, err error) {
	if err = lmc.simulate(ctx); err != nil {
		return
	}
	deleted = lmc.delete(prefix)
	return
}

func (lmc *localMemoryCoordinator) delete(prefix string) (deleted int64) {
	lmc.data.Range(func(key, value interface{}) bool {
		k := key.(string)
		if strings.HasPrefix(k, prefix) {
			lmc.data.Delete(k)
			lmc.counterLock.Lock()
			if _, ok := lmc.counter[k]; ok {
				delete(lmc.counter, k)
			}
			lmc.counterLock.Unlock()

			go lmc.notifySubscribers(WatchEvent{
				Type: DeleteEvent,
				Item: RawItem{Key: k},
			})
			deleted += 1
		}
		return true
	})
	return deleted
}

func (lmc *localMemoryCoordinator) GrantLease(ctx context.Context, ttl time.Duration) (clientv3.LeaseID, error) {
	lease := clientv3.LeaseID(rand.Uint64())
	lmc.leases.Store(lease, ttl)
	return lease, nil
}

func (lmc *localMemoryCoordinator) Watch(ctx context.Context, prefix string) chan WatchEvent {
	lmc.subsLock.Lock()
	defer lmc.subsLock.Unlock()

	eventsChan := make(chan WatchEvent)
	lmc.subscriptions = append(lmc.subscriptions, subscription{
		prefix: prefix,
		events: eventsChan,
	})
	return eventsChan
}

func (lmc *localMemoryCoordinator) notifySubscribers(ev WatchEvent) {
	lmc.subsLock.RLock()
	defer lmc.subsLock.RUnlock()

	for _, sub := range lmc.subscriptions {
		if strings.HasPrefix(ev.Item.Key, sub.prefix) {
			sub.events <- ev
		}
	}
}

func (lmc *localMemoryCoordinator) Close() error {
	lmc.subsLock.RLock()
	defer lmc.subsLock.RUnlock()

	for _, sub := range lmc.subscriptions {
		close(sub.events)
	}
	return nil
}

type localMemoryOptions struct {
	simulatedDelay time.Duration
	simulatedError error
}

type LocalMemoryOption func(*localMemoryOptions)

func WithSimulatedDelay(delay time.Duration) LocalMemoryOption {
	return func(opt *localMemoryOptions) {
		opt.simulatedDelay = delay
	}
}

func WithSimulatedError(err error) LocalMemoryOption {
	return func(opt *localMemoryOptions) {
		opt.simulatedError = err
	}
}
