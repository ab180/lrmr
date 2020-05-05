package coordinator

import "github.com/coreos/etcd/clientv3"

// Txn performs batch operation to the coordinator.Coordinator.
// To apply changes, Commit() must be called with the Txn on coordinator.
type Txn struct {
	Ops []BatchOp
}

// NewTxn returns a new transaction.
func NewTxn() *Txn {
	return &Txn{}
}

// Put performs a batch operation setting the value of a key to within the transaction.
func (t *Txn) Put(key string, value interface{}, opts ...clientv3.OpOption) *Txn {
	t.Ops = append(t.Ops, BatchOp{
		Type:    PutEvent,
		Key:     key,
		Value:   value,
		Options: opts,
	})
	return t
}

// IncrementCounter performs a batch operation incrementing counter of a key within the transaction.
func (t *Txn) IncrementCounter(key string, opts ...clientv3.OpOption) *Txn {
	t.Ops = append(t.Ops, BatchOp{
		Type:    CounterEvent,
		Key:     key,
		Options: opts,
	})
	return t
}

// Delete performs a batch operation deleting all keys starting with given prefix within the transaction.
func (t *Txn) Delete(keyPrefix string) *Txn {
	t.Ops = append(t.Ops, BatchOp{
		Type: DeleteEvent,
		Key:  keyPrefix,
	})
	return t
}
