package coordinator

import (
	"context"
	"errors"
	jsoniter "github.com/json-iterator/go"
)

var (
	ErrNotFound = errors.New("key not found")
)

type Coordinator interface {
	Get(ctx context.Context, key string, valuePtr interface{}) error
	Scan(ctx context.Context, prefix string) (results []RawItem, err error)
	Put(ctx context.Context, key string, value interface{}) error
	BatchPut(ctx context.Context, values map[string]interface{}) error
	Delete(ctx context.Context, prefix string) (deleted int64, err error)
}

// RawItem is a data of item which isn't unmarshalled yet.
type RawItem []byte

func (r RawItem) Unmarshal(value interface{}) error {
	// assuming that the value is a struct pointer
	return jsoniter.Unmarshal(r, value)
}
