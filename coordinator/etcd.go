package coordinator

import (
	"context"
	jsoniter "github.com/json-iterator/go"
	"go.etcd.io/etcd/clientv3"
)

type Etcd struct {
	client *clientv3.Client
}

func NewEtcd(cfg clientv3.Config) (Coordinator, error) {
	cli, err := clientv3.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Etcd{client: cli}, nil
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
		results = append(results, kv.Value)
	}
	return
}

func (e *Etcd) Put(ctx context.Context, key string, value interface{}) error {
	jsonVal, err := jsoniter.MarshalToString(value)
	if err != nil {
		return err
	}
	_, err = e.client.Put(ctx, key, jsonVal)
	return err
}

func (e *Etcd) BatchPut(ctx context.Context, values map[string]interface{}) error {
	var ops []clientv3.Op
	for key, value := range values {
		jsonVal, err := jsoniter.MarshalToString(value)
		if err != nil {
			return err
		}
		ops = append(ops, clientv3.OpPut(key, jsonVal))
	}
	_, err := e.client.Txn(ctx).Then(ops...).Commit()
	return err
}

func (e *Etcd) Delete(ctx context.Context, prefix string) (deleted int64, err error) {
	resp, err := e.client.Delete(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return 0, err
	}
	return resp.Deleted, nil
}
