package remote

import (
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/dataframe"
)

type masterContext struct {
	finishedInputCounts int32
	totalInputCounts    int32

	results []dataframe.Row
}

type workerJobContext struct {
	task lrmr.Task

	finishedInputCounts int32
	totalInputCounts    int32

	outputChans map[string]chan interface{}
	states      map[string]interface{}
	broadcasts  map[string]interface{}
}

func (w *workerJobContext) Broadcast(key string) interface{} {
	return w.broadcasts[key]
}
