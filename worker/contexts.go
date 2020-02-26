package worker

import (
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	transformation2 "github.com/therne/lrmr/transformation"
)

type workerJobContext struct {
	job            *node.Job
	stage          *node.Stage
	transformation transformation2.Transformation
	output         output.Output

	finishedInputCounts int32
	totalInputCounts    int32
	isRunning           bool

	states     map[string]interface{}
	broadcasts map[string]interface{}
}

func (w *workerJobContext) Broadcast(key string) interface{} {
	return w.broadcasts[key]
}
