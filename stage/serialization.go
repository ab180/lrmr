package stage

import (
	"fmt"
	"github.com/shamaton/msgpack"
)

func (st Stage) Serialize(box interface{}) []byte {
	data, err := msgpack.Encode(box)
	if err != nil {
		panic(fmt.Sprintf("broadcasting %s: %v", st.Name, err))
	}
	return data
}

func (st Stage) Deserialize(serialized []byte) (r Runner, err error) {
	box := st.NewBox()
	if len(serialized) > 0 {
		err = msgpack.Decode(serialized, box)
	}
	r = st.Constructor(box)
	return
}
