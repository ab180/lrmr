package coordinator

import (
	"context"
	"github.com/airbloc/logger"
	"github.com/coreos/etcd/mvcc/mvccpb"
	jsoniter "github.com/json-iterator/go"
	"go.etcd.io/etcd/clientv3"
)

const (
	// counterMark is value used for counter keys. If a key's value equals to counterMark,
	// it means the key is counter and its value would be its version.
	counterMark = "__counter"
)

type Etcd struct {
	client *clientv3.Client
	log    logger.Logger
}

func NewEtcd(endpoints []string) (Coordinator, error) {
	cfg := clientv3.Config{
		Endpoints: endpoints,
	}
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Etcd{
		client: cli,
		log:    logger.New("etcd"),
	}, nil
}

func (e *Etcd) Get(ctx context.Context, key string, valuePtr interface{}) error {
	resp, err := e.client.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return ErrNotFound
	}
	return jsoniter.Unmarshal(resp.Kvs[0].Value, valuePtr)
}

func (e *Etcd) Scan(ctx context.Context, prefix string) (results []RawItem, err error) {
	resp, err := e.client.Get(ctx, prefix, clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
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

	wc := e.client.Watch(ctx, prefix, clientv3.WithPrefix())
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

func (e *Etcd) Put(ctx context.Context, key string, value interface{}) error {
	jsonVal, err := jsoniter.MarshalToString(value)
	if err != nil {
		return err
	}
	_, err = e.client.Put(ctx, key, jsonVal)
	return err
}

func (e *Etcd) Batch(ctx context.Context, ops ...BatchOp) error {
	var txOps []clientv3.Op
	for _, op := range ops {
		switch op.Type {
		case PutEvent:
			jsonVal, err := jsoniter.MarshalToString(op.Value)
			if err != nil {
				return err
			}
			txOps = append(txOps, clientv3.OpPut(op.Key, jsonVal))

		case CounterEvent:
			txOps = append(txOps, clientv3.OpPut(op.Key, counterMark))
		}
	}
	_, err := e.client.Txn(ctx).Then(txOps...).Commit()
	return err
}

func (e *Etcd) IncrementCounter(ctx context.Context, key string) (counter int64, err error) {
	// uses version as a cheap atomic counter
	result, err := e.client.Put(ctx, key, counterMark, clientv3.WithPrevKV())
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
	resp, err := e.client.Get(ctx, key)
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
	resp, err := e.client.Delete(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	return resp.Deleted, nil
}
