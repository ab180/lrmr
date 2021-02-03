package serialization

import (
	"github.com/pkg/errors"
)

type Broadcast map[string]interface{}

func SerializeBroadcast(b Broadcast) (s map[string][]byte, err error) {
	s = make(map[string][]byte)
	for k, v := range b {
		s[k], err = SerializeStruct(v)
		if err != nil {
			return nil, errors.Wrapf(err, "serialize broadcast %s", k)
		}
	}
	return s, nil
}

func DeserializeBroadcast(data map[string][]byte) (Broadcast, error) {
	b := make(Broadcast)
	for k, raw := range data {
		v, err := DeserializeStruct(raw)
		if err != nil {
			return nil, errors.Wrapf(err, "deserialize broadcast %s", k)
		}
		b[k] = v
	}
	return b, nil
}
