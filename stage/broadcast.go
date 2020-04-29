package stage

import (
	"fmt"
	"github.com/shamaton/msgpack"
)

const (
	serializeKeyPrefix = "__stage/"
	broadcastKeyPrefix = "__broadcast/"
)

// Broadcasts is a set of variables broadcasted throughout the cluster during the job run.
// For example, it can contain a common configuration fed from master which needs to be used on stages.
type Broadcasts map[string]interface{}

func (b Broadcasts) Value(key string) interface{} {
	return b[broadcastKeyPrefix+key]
}

func (b Broadcasts) DeserializeStage(st Stage) (r Runner, err error) {
	box := st.NewBox()
	if serialized, ok := b[serializeKeyPrefix+st.Name]; ok {
		err = msgpack.Decode(serialized.([]byte), box)
	}
	r = st.Constructor(box)
	return
}

type BroadcastBuilder map[string][]byte

func (b *BroadcastBuilder) Put(key string, val interface{}) {
	data, err := msgpack.Encode(val)
	if err != nil {
		panic("broadcast value must be serializable: " + key)
	}
	(*b)[broadcastKeyPrefix+key] = data
}

func (b *BroadcastBuilder) SerializeStage(st Stage, runner interface{}) {
	data, err := msgpack.Encode(runner)
	if err != nil {
		panic(fmt.Sprintf("broadcasting %s: %v", st.Name, err))
	}
	// encode again so value can be []byte in stage.Broadcast
	raw, _ := msgpack.Encode(data)
	(*b)[serializeKeyPrefix+st.Name] = raw
}
