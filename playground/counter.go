package playground

import (
	"fmt"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
	"sync"
)

type Counter struct {
	counter sync.Map
}

func CountByApp() transformation.Transformation {
	return &Counter{}
}

func (cnt *Counter) Setup(c transformation.Context) error {
	return nil
}

func (cnt *Counter) Run(row lrdd.Row, out output.Output) error {
	count, _ := cnt.counter.LoadOrStore(row["appID"], 0)
	cnt.counter.Store(row["appID"], count.(int)+1)
	return nil
}

func (cnt *Counter) Teardown(out output.Output) error {
	cnt.counter.Range(func(key, value interface{}) bool {
		fmt.Printf("App %v: %v\n", key, value)
		return true
	})
	return nil
}
