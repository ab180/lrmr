package transformation

import (
	"reflect"

	"github.com/therne/lrmr/internal/serialization"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
)

type Transformation interface {
	Apply(ctx Context, in chan *lrdd.Row, out output.Output) error
}

type Serializable struct{ Transformation }

func (s Serializable) MarshalJSON() ([]byte, error) {
	return serialization.SerializeStruct(s.Transformation)
}

func (s *Serializable) UnmarshalJSON(d []byte) error {
	v, err := serialization.DeserializeStruct(d)
	if err != nil {
		return err
	}
	if v != nil {
		s.Transformation = v.(Transformation)
	}
	return nil
}

func NameOf(tf Transformation) string {
	if s, ok := tf.(Serializable); ok {
		return NameOf(s.Transformation)
	}
	return reflect.TypeOf(tf).Name()
}
