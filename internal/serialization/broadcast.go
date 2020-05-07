package serialization

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
)

type Broadcast map[string]interface{}

func SerializeBroadcast(b Broadcast) (s map[string][]byte, err error) {
	s = make(map[string][]byte)
	for k, v := range b {
		s[k], err = jsoniter.Marshal(v)
		if err != nil {
			return nil, errors.Wrapf(err, "serialize broadcast %s", k)
		}
	}
	return s, nil
}

func DeserializeBroadcast(data map[string][]byte) (Broadcast, error) {
	b := make(Broadcast)
	for k, raw := range data {
		var v interface{}
		if err := jsoniter.Unmarshal(raw, &v); err != nil {
			return nil, errors.Wrapf(err, "deserialize broadcast %s", k)
		}
		b[k] = v
	}
	return b, nil
}
