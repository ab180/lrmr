package lrmr

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/ab180/lrmr/internal/serialization"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/output"
	"github.com/ab180/lrmr/transformation"
	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
)

func RegisterTypes(tfs ...interface{}) interface{} {
	for _, tf := range tfs {
		serialization.TypeOf(tf)
	}
	return nil
}

type Transformer interface {
	Transform(ctx Context, in chan *lrdd.Row, emit func(*lrdd.Row)) error
}

type transformerTransformation struct {
	transformer Transformer
}

func (t transformerTransformation) Apply(ctx transformation.Context, in chan *lrdd.Row, out output.Output) (emitErr error) {
	childCtx, cancel := contextWithCancel(ctx)
	defer cancel()

	emit := func(row *lrdd.Row) {
		if emitErr = out.Write(row); emitErr != nil {
			cancel()
		}
	}
	if err := t.transformer.Transform(childCtx, in, emit); err != nil {
		if errors.Cause(err) == context.Canceled && emitErr != nil {
			return emitErr
		}
		return err
	}
	return emitErr
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
		if ctx.Err() != nil {
			return ctx.Err()
		}
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
		if ctx.Err() != nil {
			return ctx.Err()
		}
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
		if ctx.Err() != nil {
			return ctx.Err()
		}
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

type Combiner interface {
	MapValueToAccumulator(value *lrdd.Row) (acc MarshalerUnmarshaler)
	MergeValue(ctx Context, prevAcc MarshalerUnmarshaler, curValue *lrdd.Row) (nextAcc MarshalerUnmarshaler, err error)
	MergeAccumulator(ctx Context, prevAcc, curAcc interface{})
}

type combinerTransformation struct {
	combinerPrototype Combiner
}

func (f *combinerTransformation) Apply(c transformation.Context, in chan *lrdd.Row, out output.Output) error {
	combiners := make(map[string]Combiner)
	state := make(map[string]MarshalerUnmarshaler)

	for row := range in {
		ctx := replacePartitionKey(c, row.Key)

		combiner := combiners[row.Key]
		if combiner == nil {
			combiner = f.instantiateCombiner()
			initialState := combiner.MapValueToAccumulator(row)

			combiners[row.Key] = combiner
			state[row.Key] = initialState
			continue
		}
		next, err := combiner.MergeValue(ctx, state[row.Key], row)
		if err != nil {
			return err
		}
		state[row.Key] = next
	}

	i := 0
	rows := make([]*lrdd.Row, len(state))
	for key, finalVal := range state {
		bs, err := finalVal.MarshalMsg(nil)
		if err != nil {
			return fmt.Errorf("failed to marshal final value: %w", err)
		}
		rows[i] = &lrdd.Row{Key: key, Value: bs}
		i++
	}
	return out.Write(rows...)
}

func (f *combinerTransformation) instantiateCombiner() Combiner {
	// clone combiner object from prototype
	c := reflect.New(reflect.TypeOf(f.combinerPrototype).Elem()).Interface()
	if err := copier.Copy(c, f.combinerPrototype); err != nil {
		panic("failed to instantiate combiner: " + err.Error())
	}
	return c.(Combiner)
}

func (f *combinerTransformation) MarshalJSON() ([]byte, error) {
	return serialization.SerializeStruct(f.combinerPrototype)
}

func (f *combinerTransformation) UnmarshalJSON(data []byte) error {
	v, err := serialization.DeserializeStruct(data)
	if err != nil {
		return err
	}
	f.combinerPrototype = v.(Combiner)
	return nil
}

type Reducer interface {
	InitialValue() MarshalerUnmarshaler
	Reduce(ctx Context, prev MarshalerUnmarshaler, cur *lrdd.Row) (next MarshalerUnmarshaler, err error)
}

type MarshalerUnmarshaler interface {
	MarshalMsg([]byte) ([]byte, error)
	UnmarshalMsg([]byte) ([]byte, error)
}

type reduceTransformation struct {
	reducerPrototype Reducer
}

func (f *reduceTransformation) Apply(c transformation.Context, in chan *lrdd.Row, out output.Output) error {
	reducers := make(map[string]Reducer)
	state := make(map[string]MarshalerUnmarshaler)

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
		bs, err := finalVal.MarshalMsg(nil)
		if err != nil {
			return fmt.Errorf("failed to marshal final value: %w", err)
		}
		rows[i] = &lrdd.Row{Key: key, Value: bs}
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
