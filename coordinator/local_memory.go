package coordinator

import (
	"context"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type localMemoryCoordinator struct {
	opt    localMemoryOptions
	data   sync.Map
	leases sync.Map

	counter     map[string]int64
	counterLock sync.RWMutex

	subscriptions []subscription
	subsLock      sync.RWMutex

	optsApplied []WriteOption
}

type entry struct {
	item  RawItem
	lease clientv3.LeaseID
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
	v, ok := lmc.data.Load(key)
	if !ok {
		return ErrNotFound
	}
	e := v.(entry)
	log.Println("Key", key, "lease is", e.lease)
	if lmc.isAfterDeadline(e.lease) {
		lmc.expireLease(key, e.lease)
		return ErrNotFound
	}
	return e.item.Unmarshal(valuePtr)
}

func (lmc *localMemoryCoordinator) Scan(ctx context.Context, prefix string) (results []RawItem, err error) {
	if err := lmc.simulate(ctx); err != nil {
		return nil, err
	}
	lmc.data.Range(func(key, value interface{}) bool {
		if strings.HasPrefix(key.(string), prefix) {
			e := value.(entry)
			if lmc.isAfterDeadline(e.lease) {
				lmc.expireLease(key, e.lease)
				return true
			}
			results = append(results, e.item)
		}
		return true
	})
	return
}

func (lmc *localMemoryCoordinator) Put(ctx context.Context, key string, value interface{}, opts ...WriteOption) error {
	if err := lmc.simulate(ctx); err != nil {
		return err
	}
	opt := buildWriteOption(append(lmc.optsApplied, opts...))
	return lmc.put(key, value, opt.Lease)
}

func (lmc *localMemoryCoordinator) put(k string, v interface{}, lease clientv3.LeaseID) error {
	raw, err := jsoniter.Marshal(v)
	if err != nil {
		return err
	}
	entry := entry{
		lease: lease,
		item: RawItem{
			Key:   k,
			Value: raw,
		},
	}
	lmc.data.Store(k, entry)
	go lmc.notifySubscribers(WatchEvent{
		Type: PutEvent,
		Item: entry.item,
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

func (lmc *localMemoryCoordinator) Commit(ctx context.Context, txn *Txn, opts ...WriteOption) ([]TxnResult, error) {
	if err := lmc.simulate(ctx); err != nil {
		return nil, err
	}
	results := make([]TxnResult, len(txn.Ops))
	for i, op := range txn.Ops {
		switch op.Type {
		case PutEvent:
			opt := buildWriteOption(opts)
			if err := lmc.put(op.Key, op.Value, opt.Lease); err != nil {
				return nil, err
			}
		case CounterEvent:
			results[i].Counter = lmc.incrementCounter(op.Key)
		case DeleteEvent:
			results[i].Deleted = lmc.delete(op.Key)
		}
		results[i].Type = op.Type
	}
	return results, nil
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
	deadline := time.Now().Add(ttl)
	lmc.leases.Store(lease, deadline)
	log.Println("lease", lease, "created to", deadline)
	return lease, nil
}

func (lmc *localMemoryCoordinator) KeepAlive(ctx context.Context, lease clientv3.LeaseID) error {
	go func() {
		tick := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-tick.C:
				newDeadline := time.Now().Add(2 * time.Second)
				log.Println("lease extended to", newDeadline)
				lmc.leases.Store(lease, newDeadline)

			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (lmc *localMemoryCoordinator) isAfterDeadline(lease clientv3.LeaseID) (expired bool) {
	if lease == clientv3.NoLease {
		return false
	}
	v, ok := lmc.leases.Load(lease)
	if !ok {
		log.Println(" => but no lease", lease)
		return true
	}
	deadline := v.(time.Time)
	log.Println(" => deadline is", deadline.Format(time.RFC3339))
	return time.Now().After(deadline)
}

func (lmc *localMemoryCoordinator) expireLease(key interface{}, lease clientv3.LeaseID) {
	lmc.leases.Delete(lease)
	lmc.data.Delete(key)
}

func (lmc *localMemoryCoordinator) Watch(ctx context.Context, prefix string) chan WatchEvent {
	lmc.subsLock.Lock()
	defer lmc.subsLock.Unlock()

	eventsChan := make(chan WatchEvent, 100)
	lmc.subscriptions = append(lmc.subscriptions, subscription{
		prefix: prefix,
		events: eventsChan,
	})
	go func() {
		select {
		case <-ctx.Done():
			lmc.subsLock.Lock()
			for i, sub := range lmc.subscriptions {
				if sub.events == eventsChan {
					lmc.subscriptions = append(lmc.subscriptions[:i], lmc.subscriptions[i+1:]...)
					break
				}
			}
			close(eventsChan)
			lmc.subsLock.Unlock()
		}
	}()
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

func (lmc *localMemoryCoordinator) WithOptions(opts ...WriteOption) KV {
	return &childLocalMemoryCoordinator{
		localMemoryCoordinator: lmc,
		overrideOptions:        opts,
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

type childLocalMemoryCoordinator struct {
	*localMemoryCoordinator
	overrideOptions []WriteOption
}

func (c *childLocalMemoryCoordinator) Put(ctx context.Context, key string, value interface{}, opts ...WriteOption) error {
	original := c.localMemoryCoordinator.optsApplied
	c.localMemoryCoordinator.optsApplied = append(original, c.overrideOptions...)
	defer func() {
		c.localMemoryCoordinator.optsApplied = original
	}()
	return c.localMemoryCoordinator.Put(ctx, key, value, opts...)
}
