package coordinator

import jsoniter "github.com/json-iterator/go"

// EventType is the type of the events from watching keys.
type EventType int

const (
	PutEvent EventType = iota
	DeleteEvent
	CounterEvent
)

type WatchEvent struct {
	Type EventType
	Item RawItem

	// Counter is a new value of the counter when the event is CounterEvent.
	Counter int64
}

// RawItem is a data of item which isn't unmarshalled yet.
type RawItem struct {
	Key   string
	Value []byte
}

func (r RawItem) Unmarshal(value interface{}) error {
	// assuming that the value is a struct pointer
	return jsoniter.Unmarshal(r.Value, value)
}

type BatchOp struct {
	Type  EventType
	Key   string
	Value interface{}
}
