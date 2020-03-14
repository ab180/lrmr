package playground

import (
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
)

var log = logger.New("playground")
var _ = stage.RegisterReducer("Count", Count())

type counter struct {
	value uint64
}

func Count() stage.Reducer {
	return &counter{}
}

func (cnt *counter) InitialValue() interface{} {
	return uint64(0)
}

func (cnt *counter) Reduce(c stage.Context, prev interface{}, cur *lrdd.Row) (next interface{}, err error) {
	cnt.value = prev.(uint64) + 1
	return cnt.value, nil
}

func (cnt *counter) Setup(c stage.Context) error {
	return nil
}

func (cnt *counter) Teardown(c stage.Context, out output.Writer) error {
	c.AddMetric("Events", int(cnt.value))
	log.Info("App {}: {}", c.PartitionKey(), cnt.value)
	return nil
}
