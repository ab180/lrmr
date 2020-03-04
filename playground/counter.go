package playground

import (
	"fmt"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
)

type Counter struct {
	counters []map[string]int
}

func CountByApp() transformation.Transformation {
	return &Counter{}
}

func (cnt *Counter) Setup(c transformation.Context) error {
	cnt.counters = make([]map[string]int, c.NumExecutors())
	for i := range cnt.counters {
		cnt.counters[i] = make(map[string]int)
	}
	return nil
}

func (cnt *Counter) Apply(c transformation.Context, row lrdd.Row, out output.Output) error {
	c.AddCustomMetric("Events", 1)
	counter := cnt.counters[c.CurrentExecutor()]
	counter[row["appID"].(string)] += 1
	return nil
}

func (cnt *Counter) Teardown(c transformation.Context) error {
	summary := make(map[string]int)
	for _, counter := range cnt.counters {
		for appID, count := range counter {
			summary[appID] += count
		}
	}
	for appID, count := range summary {
		fmt.Printf("App %v: %v\n", appID, count)
	}
	return nil
}
