package coordinator

import (
	"context"
	"time"

	"github.com/airbloc/logger"
	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/clientv3/namespace"
	"github.com/coreos/etcd/mvcc/mvccpb"
	jsoniter "github.com/json-iterator/go"
	"google.golang.org/grpc"
)

const (
	// counterMark is value used for counter keys. If a key's value equals to counterMark,
	// it means the key is counter and its value would be its version.
	counterMark = "__counter"
)

type Etcd struct {
	Client  *clientv3.Client
	KV      clientv3.KV
	Watcher clientv3.Watcher
	Lease   clientv3.Lease

	log  logger.Logger
	opts []WriteOption
}

func NewEtcd(endpoints []string, nsPrefix string) (Coordinator, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithBlock()},
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
	}, nil
}

func (e *Etcd) Get(ctx context.Context, key string, valuePtr interface{}) error {
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
		for wr := range wc {
			if err := wr.Err(); err != nil {
				e.log.Error("watch error", err)
				continue
			}
			for _, e := range wr.Events {
				switch e.Type {
				case mvccpb.PUT:
					if string(e.Kv.Value) == counterMark {
						watchChan <- WatchEvent{
							Type:    CounterEvent,
							Item:    RawItem{Key: string(e.Kv.Key)},
							Counter: e.Kv.Version,
						}
						continue
					}
					watchChan <- WatchEvent{
						Type: PutEvent,
						Item: RawItem{
							Key:   string(e.Kv.Key),
							Value: e.Kv.Value,
						},
					}

				case mvccpb.DELETE:
					watchChan <- WatchEvent{
						Type: DeleteEvent,
						Item: RawItem{Key: string(e.Kv.Key)},
					}
				}
			}
		}
	}()
	return watchChan
}

func (e *Etcd) Put(ctx context.Context, key string, value interface{}, opts ...WriteOption) error {
	jsonVal, err := jsoniter.MarshalToString(value)
	if err != nil {
		return err
	}
	var etcdOpts []clientv3.OpOption
	opt := buildWriteOption(append(e.opts, opts...))
	if opt.Lease != clientv3.NoLease {
		etcdOpts = append(etcdOpts, clientv3.WithLease(opt.Lease))
	}
	_, err = e.KV.Put(ctx, key, jsonVal, etcdOpts...)
	return err
}

func (e *Etcd) Commit(ctx context.Context, txn *Txn, opts ...WriteOption) error {
	var etcdOpts []clientv3.OpOption
	opt := buildWriteOption(append(e.opts, opts...))
	if opt.Lease != clientv3.NoLease {
		etcdOpts = append(etcdOpts, clientv3.WithLease(opt.Lease))
	}

	var txOps []clientv3.Op
	for _, op := range txn.Ops {
		switch op.Type {
		case PutEvent:
			jsonVal, err := jsoniter.MarshalToString(op.Value)
			if err != nil {
				return err
			}
			txOps = append(txOps, clientv3.OpPut(op.Key, jsonVal, etcdOpts...))

		case CounterEvent:
			txOps = append(txOps, clientv3.OpPut(op.Key, counterMark, etcdOpts...))

		case DeleteEvent:
			txOps = append(txOps, clientv3.OpDelete(op.Key, clientv3.WithPrefix()))
		}
	}
	_, err := e.KV.Txn(ctx).Then(txOps...).Commit()
	return err
}

func (e *Etcd) GrantLease(ctx context.Context, ttl time.Duration) (clientv3.LeaseID, error) {
	lease, err := e.Lease.Grant(ctx, int64(ttl.Seconds()))
	if err != nil {
		return 0, err
	}
	return lease.ID, nil
}

func (e *Etcd) KeepAlive(ctx context.Context, lease clientv3.LeaseID) error {
	_, err := e.Lease.KeepAlive(ctx, lease)
	return err
}

func (e *Etcd) IncrementCounter(ctx context.Context, key string) (counter int64, err error) {
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

func (e *Etcd) ReadCounter(ctx context.Context, key string) (counter int64, err error) {
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

func (e *Etcd) Delete(ctx context.Context, prefix string) (deleted int64, err error) {
	resp, err := e.KV.Delete(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	return resp.Deleted, nil
}

func (e *Etcd) WithOptions(opt ...WriteOption) KV {
	return &Etcd{
		Client:  e.Client,
		KV:      e.KV,
		Watcher: e.Watcher,
		Lease:   e.Lease,
		log:     logger.New("etcd"),
		opts:    opt,
	}
}

func (e *Etcd) Close() error {
	return e.Client.Close()
}
