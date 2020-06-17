package lrmr

import (
	"context"
	"reflect"
	"sort"

	"github.com/jinzhu/copier"
	"github.com/therne/lrmr/internal/serialization"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
)

func RegisterTypes(tfs ...interface{}) interface{} {
	for _, tf := range tfs {
		serialization.RegisterType(tf)
	}
	return nil
}

type Transformer interface {
	Transform(ctx Context, in chan *lrdd.Row, emit func(*lrdd.Row)) error
}

type transformerTransformation struct {
	transformer Transformer
}

func (t transformerTransformation) Apply(ctx transformation.Context, in chan *lrdd.Row, out output.Output) (err error) {
	childCtx, cancel := contextWithCancel(ctx)
	defer cancel()

	emit := func(row *lrdd.Row) {
		if err = out.Write(row); err != nil {
			cancel()
			close(in)
		}
	}
	retErr := t.transformer.Transform(childCtx, in, emit)
	if retErr != nil {
		if err != nil && retErr == context.Canceled {
			return err
		}
		return retErr
	}
	return nil
}

func (t *transformerTransformation) UnmarshalJSON(d []byte) error {
	transformer, err := serialization.DeserializeStruct(d)
	if err != nil {
		return err
	}
	t.transformer = transformer.(Transformer)
	return nil
}

func (t transformerTransformation) MarshalJSON() ([]byte, error) {
	return serialization.SerializeStruct(t.transformer)
}

type Filter interface {
	Filter(*lrdd.Row) bool
}

type filterTransformation struct {
	filter Filter
}

func (f filterTransformation) Apply(ctx transformation.Context, in chan *lrdd.Row, out output.Output) error {
	for row := range in {
		if !f.filter.Filter(row) {
			continue
		}
		if err := out.Write(row); err != nil {
			return err
		}
	}
	return nil
}

type Mapper interface {
	Map(Context, *lrdd.Row) (*lrdd.Row, error)
}

type mapTransformation struct {
	mapper Mapper
}

func (m *mapTransformation) Apply(ctx transformation.Context, in chan *lrdd.Row, out output.Output) error {
	for row := range in {
		outRow, err := m.mapper.Map(ctx, row)
		if err != nil {
			return err
		}
		if err := out.Write(outRow); err != nil {
			return err
		}
	}
	return nil
}

func (m *mapTransformation) MarshalJSON() ([]byte, error) {
	return serialization.SerializeStruct(m.mapper)
}

func (m *mapTransformation) UnmarshalJSON(data []byte) error {
	mapper, err := serialization.DeserializeStruct(data)
	if err != nil {
		return err
	}
	m.mapper = mapper.(Mapper)
	return nil
}

type FlatMapper interface {
	FlatMap(Context, *lrdd.Row) ([]*lrdd.Row, error)
}

type flatMapTransformation struct {
	flatMapper FlatMapper
}

func (f *flatMapTransformation) Apply(ctx transformation.Context, in chan *lrdd.Row, out output.Output) error {
	for row := range in {
		outRows, err := f.flatMapper.FlatMap(ctx, row)
		if err != nil {
			return err
		}
		if err := out.Write(outRows...); err != nil {
			return err
		}
	}
	return nil
}

func (f *flatMapTransformation) MarshalJSON() ([]byte, error) {
	return serialization.SerializeStruct(f.flatMapper)
}

func (f *flatMapTransformation) UnmarshalJSON(data []byte) error {
	mapper, err := serialization.DeserializeStruct(data)
	if err != nil {
		return err
	}
	f.flatMapper = mapper.(FlatMapper)
	return nil
}

type Sorter interface {
	IsLessThan(a, b *lrdd.Row) bool
}

type sortTransformation struct {
	sorter Sorter
	rows   []*lrdd.Row
}

func (s *sortTransformation) Apply(ctx transformation.Context, in chan *lrdd.Row, out output.Output) error {
	for row := range in {
		s.rows = append(s.rows, row)
	}
	// implemented sort.Interface by self
	sort.Sort(s)
	return out.Write(s.rows...)
}

func (s *sortTransformation) Len() int {
	return len(s.rows)
}

func (s *sortTransformation) Less(i, j int) bool {
	return s.sorter.IsLessThan(s.rows[i], s.rows[j])
}

func (s *sortTransformation) Swap(i, j int) {
	s.rows[i], s.rows[j] = s.rows[j], s.rows[i]
}

func (s *sortTransformation) MarshalJSON() ([]byte, error) {
	return serialization.SerializeStruct(s.sorter)
}

func (s *sortTransformation) UnmarshalJSON(data []byte) error {
	sorter, err := serialization.DeserializeStruct(data)
	if err != nil {
		return err
	}
	s.sorter = sorter.(Sorter)
	return nil
}

type Reducer interface {
	InitialValue() interface{}
	Reduce(ctx Context, prev interface{}, cur *lrdd.Row) (next interface{}, err error)
}

type reduceTransformation struct {
	reducerPrototype Reducer
}

func (f *reduceTransformation) Apply(c transformation.Context, in chan *lrdd.Row, out output.Output) error {
	reducers := make(map[string]Reducer)
	state := make(map[string]interface{})

	for row := range in {
		ctx := replacePartitionKey(c, row.Key)
		prev := state[row.Key]
		if reducers[row.Key] == nil {
			reducers[row.Key] = f.instantiateReducer()
			prev = reducers[row.Key].InitialValue()
		}
		next, err := reducers[row.Key].Reduce(ctx, prev, row)
		if err != nil {
			return err
		}
		state[row.Key] = next
	}

	i := 0
	rows := make([]*lrdd.Row, len(state))
	for key, finalVal := range state {
		rows[i] = lrdd.KeyValue(key, finalVal)
		i++
	}
	return out.Write(rows...)
}

func (f *reduceTransformation) instantiateReducer() Reducer {
	// clone reducer object from prototype
	r := reflect.New(reflect.TypeOf(f.reducerPrototype).Elem()).Interface()
	if err := copier.Copy(r, f.reducerPrototype); err != nil {
		panic("failed to instantiate reducer: " + err.Error())
	}
	return r.(Reducer)
}

func (f *reduceTransformation) MarshalJSON() ([]byte, error) {
	return serialization.SerializeStruct(f.reducerPrototype)
}

func (f *reduceTransformation) UnmarshalJSON(data []byte) error {
	mapper, err := serialization.DeserializeStruct(data)
	if err != nil {
		return err
	}
	f.reducerPrototype = mapper.(Reducer)
	return nil
}

type partitionKeyContext struct {
	Context
	partitionKey string
}

func replacePartitionKey(old Context, key string) (new Context) {
	return &partitionKeyContext{
		Context:      old,
		partitionKey: key,
	}
}

func (pc partitionKeyContext) PartitionKey() string {
	return pc.partitionKey
}
