package coordinator

import (
	"context"
	"time"

	"github.com/airbloc/logger"
	"github.com/creasty/defaults"
	jsoniter "github.com/json-iterator/go"
	"github.com/therne/errorist"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

const (
	// counterMark is value used for counter keys. If a key's value equals to counterMark,
	// it means the key is counter and its value would be its version.
	counterMark = "__counter"

	// below settings is for disaster recovery / distributed cluster
	keepAliveInterval = 5 * time.Second
	keepAliveTimeout  = 3 * time.Second
	autoSyncInterval  = time.Minute
)

type Etcd struct {
	Client  *clientv3.Client
	KV      clientv3.KV
	Watcher clientv3.Watcher
	Lease   clientv3.Lease

	log    logger.Logger
	option EtcdOptions
}

type EtcdOptions struct {
	DialTimeout  time.Duration `default:"5s"`
	OpTimeout    time.Duration `default:"3s"`
	WriteOptions []WriteOption
}

func defaultEtcdOptions() (o EtcdOptions) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	return
}

func NewEtcd(endpoints []string, nsPrefix string, opts ...EtcdOptions) (Coordinator, error) {
	option := defaultEtcdOptions()
	if len(opts) > 0 {
		option = opts[0]
	}

	cfg := clientv3.Config{
		Endpoints:            endpoints,
		DialTimeout:          option.DialTimeout,
		DialOptions:          []grpc.DialOption{grpc.WithBlock()},
		DialKeepAliveTime:    keepAliveInterval,
		DialKeepAliveTimeout: keepAliveTimeout,
		AutoSyncInterval:     autoSyncInterval,
		PermitWithoutStream:  true,
		Logger:               zap.NewNop(),
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Etcd{
		Client:  cli,
		KV:      namespace.NewKV(cli, nsPrefix),
		Watcher: namespace.NewWatcher(cli, nsPrefix),
		Lease:   namespace.NewLease(cli, nsPrefix),
		log:     logger.New("etcd"),
		option:  option,
	}, nil
}

func (e *Etcd) Get(ctx context.Context, key string, valuePtr interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, e.option.OpTimeout)
	defer cancel()

	resp, err := e.KV.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return ErrNotFound
	}
	return jsoniter.Unmarshal(resp.Kvs[0].Value, valuePtr)
}

func (e *Etcd) Scan(ctx context.Context, prefix string) (results []RawItem, err error) {
	ctx, cancel := context.WithTimeout(ctx, e.option.OpTimeout)
	defer cancel()

	resp, err := e.KV.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
	if err != nil {
		return
	}
	for _, kv := range resp.Kvs {
		results = append(results, RawItem{
			Key:   string(kv.Key),
			Value: kv.Value,
		})
	}
	return
}

func (e *Etcd) Watch(ctx context.Context, prefix string) chan WatchEvent {
	watchChan := make(chan WatchEvent)

	wc := e.Watcher.Watch(ctx, prefix, clientv3.WithPrefix())
	go func() {
		defer func() {
			if err := errorist.WrapPanic(recover()); err != nil {
				e.log.Error("Panic occurred while watching prefix {}", err, prefix)
			}
		}()
		defer close(watchChan)
		for wr := range wc {
			if err := wr.Err(); err != nil {
				e.log.Error("watch error", err)
				continue
			}
			for _, e := range wr.Events {
				var event WatchEvent
				switch e.Type {
				case mvccpb.PUT:
					if string(e.Kv.Value) == counterMark {
						event = WatchEvent{
							Type:    CounterEvent,
							Item:    RawItem{Key: string(e.Kv.Key)},
							Counter: e.Kv.Version,
						}
					} else {
						event = WatchEvent{
							Type: PutEvent,
							Item: RawItem{
								Key:   string(e.Kv.Key),
								Value: e.Kv.Value,
							},
						}
					}

				case mvccpb.DELETE:
					event = WatchEvent{
						Type: DeleteEvent,
						Item: RawItem{Key: string(e.Kv.Key)},
					}

				default:
					continue
				}

				select {
				case watchChan <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return watchChan
}

func (e *Etcd) Put(ctx context.Context, key string, value interface{}, opts ...WriteOption) error {
	ctx, cancel := context.WithTimeout(ctx, e.option.OpTimeout)
	defer cancel()

	jsonVal, err := jsoniter.MarshalToString(value)
	if err != nil {
		return err
	}
	var etcdOpts []clientv3.OpOption
	opt := buildWriteOption(append(e.option.WriteOptions, opts...))
	if opt.Lease != clientv3.NoLease {
		etcdOpts = append(etcdOpts, clientv3.WithLease(opt.Lease))
	}
	_, err = e.KV.Put(ctx, key, jsonVal, etcdOpts...)
	return err
}

func (e *Etcd) CAS(ctx context.Context, key string, old interface{}, new interface{}, opts ...WriteOption,
) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, e.option.OpTimeout)
	defer cancel()

	var cmp clientv3.Cmp
	if old != nil {
		oldB, err := jsoniter.MarshalToString(old)
		if err != nil {
			return false, err
		}
		cmp = clientv3.Compare(clientv3.Value(key), "=", oldB)
	} else {
		cmp = clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	}

	var etcdOpts []clientv3.OpOption
	opt := buildWriteOption(append(e.option.WriteOptions, opts...))
	if opt.Lease != clientv3.NoLease {
		etcdOpts = append(etcdOpts, clientv3.WithLease(opt.Lease))
	}

	var op clientv3.Op
	if new != nil {
		newJSON, err := jsoniter.MarshalToString(new)
		if err != nil {
			return false, err
		}
		op = clientv3.OpPut(key, newJSON, etcdOpts...)
	} else {
		op = clientv3.OpDelete(key)
	}

	res, err := e.KV.Txn(ctx).If(cmp).Then(op).Commit()
	if err != nil {
		return false, err
	}

	return res.Succeeded, nil
}

func (e *Etcd) Commit(ctx context.Context, txn *Txn, opts ...WriteOption) ([]TxnResult, error) {
	ctx, cancel := context.WithTimeout(ctx, e.option.OpTimeout)
	defer cancel()

	var etcdOpts []clientv3.OpOption
	opt := buildWriteOption(append(e.option.WriteOptions, opts...))
	if opt.Lease != clientv3.NoLease {
		etcdOpts = append(etcdOpts, clientv3.WithLease(opt.Lease))
	}

	var txOps []clientv3.Op
	for _, op := range txn.Ops {
		switch op.Type {
		case PutEvent:
			jsonVal, err := jsoniter.MarshalToString(op.Value)
			if err != nil {
				return nil, err
			}
			txOps = append(txOps, clientv3.OpPut(op.Key, jsonVal, etcdOpts...))

		case CounterEvent:
			countOpts := append(etcdOpts, clientv3.WithPrevKV())
			txOps = append(txOps, clientv3.OpPut(op.Key, counterMark, countOpts...))

		case DeleteEvent:
			txOps = append(txOps, clientv3.OpDelete(op.Key, clientv3.WithPrefix()))
		}
	}
	etcdTxnResults, err := e.KV.Txn(ctx).Then(txOps...).Commit()
	if err != nil {
		return nil, err
	}
	results := make([]TxnResult, len(etcdTxnResults.Responses))
	for i, res := range etcdTxnResults.Responses {
		results[i].Type = txn.Ops[i].Type

		// fill transaction result by type
		switch txn.Ops[i].Type {
		case PutEvent:

		case CounterEvent:
			prevKv := res.GetResponsePut().PrevKv
			if prevKv == nil {
				results[i].Counter = 1
			} else {
				results[i].Counter = prevKv.Version + 1
			}

		case DeleteEvent:
			results[i].Deleted = res.GetResponseDeleteRange().Deleted
		}
	}
	return results, err
}

func (e *Etcd) GrantLease(ctx context.Context, ttl time.Duration) (clientv3.LeaseID, error) {
	lease, err := e.Lease.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return 0, err
	}
	return lease.ID, nil
}

func (e *Etcd) KeepAlive(ctx context.Context, lease clientv3.LeaseID) (<-chan struct{}, error) {
	resp, err := e.Lease.KeepAlive(ctx, lease)
	if err != nil {
		return nil, err
	}

	sig := make(chan struct{}, 1)
	go func() {
		defer close(sig)
		for range resp {
			// drain KeepAlive response channel
		}
	}()
	return sig, nil
}

// IncrementCounter is an atomic operation increasing the counter in given key.
// returns a increased value of the counter right after the operation.
func (e *Etcd) IncrementCounter(ctx context.Context, key string) (counter int64, err error) {
	ctx, cancel := context.WithTimeout(ctx, e.option.OpTimeout)
	defer cancel()

	// uses version as a cheap atomic counter
	result, err := e.KV.Put(ctx, key, counterMark, clientv3.WithPrevKV())
	if err != nil {
		return
	}
	if result.PrevKv == nil {
		counter = 1
		return
	}
	counter = result.PrevKv.Version + 1
	return
}

// ReadCounter
func (e *Etcd) ReadCounter(ctx context.Context, key string) (counter int64, err error) {
	ctx, cancel := context.WithTimeout(ctx, e.option.OpTimeout)
	defer cancel()

	resp, err := e.KV.Get(ctx, key)
	if err != nil {
		return
	}
	if len(resp.Kvs) == 0 {
		counter = 0
		return
	}
	if string(resp.Kvs[0].Value) != counterMark {
		err = ErrNotCounter
		return
	}
	counter = resp.Kvs[0].Version
	return
}

// Delete remove all keys starting with given prefix.
func (e *Etcd) Delete(ctx context.Context, prefix string) (deleted int64, err error) {
	ctx, cancel := context.WithTimeout(ctx, e.option.OpTimeout)
	defer cancel()

	var opts []clientv3.OpOption
	if prefix == "" {
		prefix = "\x00"
		opts = append(opts, clientv3.WithFromKey())
	} else {
		opts = append(opts, clientv3.WithPrefix())
	}
	resp, err := e.KV.Delete(ctx, prefix, opts...)
	if err != nil {
		return 0, err
	}
	return resp.Deleted, nil
}

// WithOptions returns a child etcd coordinator with given options applied.
func (e *Etcd) WithOptions(opt ...WriteOption) KV {
	child := *e
	child.option.WriteOptions = append(e.option.WriteOptions, opt...)
	return &child
}

func (e *Etcd) Close() error {
	return e.Client.Close()
}
