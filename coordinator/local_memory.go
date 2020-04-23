package coordinator

import (
	"context"
	"github.com/coreos/etcd/clientv3"
	jsoniter "github.com/json-iterator/go"
	"strings"
	"sync"
	"time"
)

type localMemoryCoordinator struct {
	opt  localMemoryOptions
	data sync.Map

	counter     map[string]int64
	counterLock sync.RWMutex

	subscriptions []subscription
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
	return lmc.incrementCounter(key)
}

func (lmc *localMemoryCoordinator) incrementCounter(key string) (count int64, err error) {
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
	return
}

func (lmc *localMemoryCoordinator) ReadCounter(ctx context.Context, key string) (count int64, err error) {
	if err := lmc.simulate(ctx); err != nil {
		return 0, err
	}
	lmc.counterLock.RLock()
	defer lmc.counterLock.RUnlock()
	return lmc.counter[key], nil
}

func (lmc *localMemoryCoordinator) Batch(ctx context.Context, ops ...BatchOp) error {
	if err := lmc.simulate(ctx); err != nil {
		return err
	}
	for _, op := range ops {
		switch op.Type {
		case PutEvent:
			if err := lmc.put(op.Key, op.Value); err != nil {
				return err
			}
		case CounterEvent:
			if _, err := lmc.incrementCounter(op.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (lmc *localMemoryCoordinator) Delete(ctx context.Context, prefix string) (deleted int64, err error) {
	lmc.data.Range(func(key, value interface{}) bool {
		k := key.(string)
		if strings.HasPrefix(k, prefix) {
			lmc.data.Delete(k)
			lmc.counterLock.Lock()
			if _, ok := lmc.counter[k]; ok {
				delete(lmc.counter, k)
			}
			lmc.counterLock.Unlock()
			deleted += 1
		}
		return true
	})
	return
}

func (lmc *localMemoryCoordinator) GrantLease(ctx context.Context, ttl time.Duration) (clientv3.LeaseID, error) {
	panic("implement me")
}

func (lmc *localMemoryCoordinator) Watch(ctx context.Context, prefix string) chan WatchEvent {
	eventsChan := make(chan WatchEvent)
	lmc.subscriptions = append(lmc.subscriptions, subscription{
		prefix: prefix,
		events: eventsChan,
	})
	return eventsChan
}

func (lmc *localMemoryCoordinator) notifySubscribers(ev WatchEvent) {
	for _, sub := range lmc.subscriptions {
		if strings.HasPrefix(ev.Item.Key, sub.prefix) {
			sub.events <- ev
		}
	}
}

func (lmc *localMemoryCoordinator) Close() error {
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
