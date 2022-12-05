package lrmr

import (
	"context"
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
	Transform(ctx Context, in chan []*lrdd.Row, emit EmitFunc) error
	RowType() lrdd.RowType
}

type transformerTransformation struct {
	transformer Transformer
}

func (t transformerTransformation) Apply(ctx transformation.Context, in chan []*lrdd.Row, out output.Output,
) (emitErr error) {
	childCtx, cancel := contextWithCancel(ctx)
	defer cancel()

	emit := func(rows []*lrdd.Row) {
		if emitErr = out.Write(rows); emitErr != nil {
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

func (t transformerTransformation) RowType() lrdd.RowType {
	return t.transformer.RowType()
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
	Filter(*lrdd.RawRow) bool
}

type Mapper interface {
	Map(Context, []*lrdd.Row) ([]*lrdd.Row, error)
	RowType() lrdd.RowType
}

type mapTransformation struct {
	mapper Mapper
}

func (m *mapTransformation) Apply(ctx transformation.Context, in chan []*lrdd.Row, out output.Output) error {
	for rows := range in {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		outRows, err := m.mapper.Map(ctx, rows)
		if err != nil {
			return err
		}
		if err := out.Write(outRows); err != nil {
			return err
		}
	}
	return nil
}

func (m *mapTransformation) RowType() lrdd.RowType {
	return m.mapper.RowType()
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
	FlatMap(Context, []*lrdd.Row) ([]*lrdd.Row, error)
	RowType() lrdd.RowType
}

type flatMapTransformation struct {
	flatMapper FlatMapper
}

func (f *flatMapTransformation) Apply(ctx transformation.Context, in chan []*lrdd.Row, out output.Output) error {
	for rows := range in {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		outRows, err := f.flatMapper.FlatMap(ctx, rows)
		if err != nil {
			return err
		}
		if err := out.Write(outRows); err != nil {
			return err
		}
	}
	return nil
}

func (f *flatMapTransformation) RowType() lrdd.RowType {
	return f.flatMapper.RowType()
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
	RowType() lrdd.RowType
}

type sortTransformation struct {
	sorter Sorter
	rows   []*lrdd.Row
}

func (s *sortTransformation) Apply(ctx transformation.Context, in chan []*lrdd.Row, out output.Output) error {
	for rows := range in {
		for _, row := range rows {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.rows = append(s.rows, row)
		}
	}
	// implemented sort.Interface by self
	sort.Sort(s)
	return out.Write(s.rows)
}

func (s *sortTransformation) RowType() lrdd.RowType {
	return s.sorter.RowType()
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
	MapValueToAccumulator(value *lrdd.RawRow) (acc *lrdd.Row)
	MergeValue(ctx Context, prevAcc *lrdd.Row, curValue *lrdd.RawRow) (nextAcc *lrdd.Row, err error)
	MergeAccumulator(ctx Context, prevAcc, curAcc interface{})
}

type Reducer interface {
	InitialValue() lrdd.MarshalUnmarshaler
	Reduce(ctx Context, prev lrdd.MarshalUnmarshaler, cur *lrdd.Row) (next lrdd.MarshalUnmarshaler, err error)
	RowType() lrdd.RowType
}

type reduceTransformation struct {
	reducerPrototype Reducer
}

func (f *reduceTransformation) Apply(c transformation.Context, in chan []*lrdd.Row, out output.Output) error {
	reducers := make(map[string]Reducer)
	state := make(map[string]lrdd.MarshalUnmarshaler)

	for rows := range in {
		for _, row := range rows {
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
	}

	i := 0
	rows := make([]*lrdd.Row, len(state))
	for key, finalVal := range state {
		rows[i] = &lrdd.Row{Key: key, Value: finalVal}
		i++
	}
	return out.Write(rows)
}

func (f *reduceTransformation) RowType() lrdd.RowType {
	return f.reducerPrototype.RowType()
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
