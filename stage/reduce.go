package stage

import (
	"github.com/jinzhu/copier"
	"github.com/modern-go/reflect2"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
)

type Reducer interface {
	InitialValue() interface{}
	Reduce(c Context, prev interface{}, cur *lrdd.Row) (next interface{}, err error)
}

func RegisterReducer(name string, r Reducer) (s Stage) {
	s = Stage{
		Name:    name,
		BoxType: typeOf(r),
		Constructor: func(boxed interface{}) Runner {
			return &reduceStage{
				reducerType:      reflect2.Type2(typeOf(boxed)),
				reducerPrototype: boxed.(Reducer),
				reducers:         make(map[string]Reducer),
				prev:             make(map[string]interface{}),
			}
		},
	}
	register(s)
	return
}

type reduceStage struct {
	reducerType      reflect2.Type
	reducerPrototype Reducer

	reducers map[string]Reducer
	prev     map[string]interface{}
}

func (rs *reduceStage) Setup(c Context) error { return nil }

func (rs *reduceStage) instantiateReducer(c Context) error {
	// clone reducer object from prototype
	r := reflect2.Type2(typeOf(rs.reducerPrototype)).New()
	if err := copier.Copy(r, rs.reducerPrototype); err != nil {
		panic("failed to instantiate reducer: " + err.Error())
	}
	reducer := r.(Reducer)
	rs.reducers[c.PartitionKey()] = reducer
	rs.prev[c.PartitionKey()] = reducer.InitialValue()

	if b, ok := reducer.(Bootstrapper); ok {
		return b.Setup(c)
	}
	return nil
}

func (rs *reduceStage) Apply(c Context, rows []*lrdd.Row, out output.Output) error {
	for _, row := range rows {
		ctx := replacePartitionKey(c, row.Key)
		if rs.reducers[row.Key] == nil {
			if err := rs.instantiateReducer(ctx); err != nil {
				return errors.Wrapf(err, "setup reducer for %s", row.Key)
			}
		}
		next, err := rs.reducers[row.Key].Reduce(ctx, rs.prev[row.Key], row)
		if err != nil {
			return err
		}
		rs.prev[row.Key] = next
	}
	return nil
}

func (rs *reduceStage) Teardown(c Context, out output.Output) error {
	var rows []*lrdd.Row
	for partitionKey, finalVal := range rs.prev {
		rows = append(rows, lrdd.KeyValue(partitionKey, finalVal))
	}
	if err := out.Write(rows); err != nil {
		return err
	}
	for partitionKey, reducer := range rs.reducers {
		if b, ok := reducer.(Bootstrapper); ok {
			ctx := replacePartitionKey(c, partitionKey)
			if err := b.Teardown(ctx, out); err != nil {
				return errors.Wrapf(err, "teardown reducer for %s", partitionKey)
			}
		}
	}
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
