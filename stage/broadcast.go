package stage

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"strings"
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
		err = jsoniter.Unmarshal(serialized.([]byte), box)
	}
	r = st.Constructor(box)
	return
}

type SerializedBroadcasts map[string][]byte

func (b *SerializedBroadcasts) Put(key string, val interface{}) {
	data, err := jsoniter.Marshal(val)
	if err != nil {
		panic("broadcast value must be serializable: " + key)
	}
	(*b)[broadcastKeyPrefix+key] = data
}

func (b *SerializedBroadcasts) SerializeStage(st Stage, runner interface{}) {
	data, err := jsoniter.Marshal(runner)
	if err != nil {
		panic(fmt.Sprintf("broadcasting %s: %v", st.Name, err))
	}
	(*b)[serializeKeyPrefix+st.Name] = data
}

func (b SerializedBroadcasts) Deserialize() (Broadcasts, error) {
	br := make(Broadcasts)
	for key, raw := range b {
		var v interface{}
		if strings.HasPrefix(key, serializeKeyPrefix) {
			// should be lazily unmarshalled in DeserializeStage.
			v = raw
		} else if err := jsoniter.Unmarshal(raw, &v); err != nil {
			return nil, errors.Wrapf(err, "deserialize broadcast %s", key)
		}
		br[key] = v
	}
	return br, nil
}
