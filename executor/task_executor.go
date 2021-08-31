package executor

import (
	"context"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/input"
	"github.com/ab180/lrmr/internal/serialization"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/output"
	"github.com/ab180/lrmr/transformation"
	"github.com/therne/errorist"
)

type TaskExecutor struct {
	task *job.Task
	job  *runningJobHolder

	Input    *input.Reader
	function transformation.Transformation
	Output   *output.Writer

	broadcast    serialization.Broadcast
	localOptions map[string]interface{}

	taskReporter *job.TaskReporter
	taskError    error
}

func NewTaskExecutor(
	clusterState cluster.State,
	runningJob *runningJobHolder,
	task *job.Task,
	status *job.TaskStatus,
	fn transformation.Transformation,
	in *input.Reader,
	out *output.Writer,
	broadcast serialization.Broadcast,
	localOptions map[string]interface{},
) *TaskExecutor {
	exec := &TaskExecutor{
		task:         task,
		job:          runningJob,
		Input:        in,
		function:     fn,
		Output:       out,
		broadcast:    broadcast,
		localOptions: localOptions,
		taskReporter: job.NewTaskReporter(clusterState, runningJob.Job, task.ID(), status),
	}
	return exec
}

func (e *TaskExecutor) Run() {
	ctx, cancel := newTaskContextWithCancel(e.job.Context(), e)
	defer cancel()

	go e.taskReporter.Start(ctx)
	defer e.reportStatus(ctx)

	// pipe input.Reader.C to function input channel
	funcInputChan := make(chan *lrdd.Row, e.Output.NumOutputs())
	go pipeAndFlattenInputs(ctx, e.Input.C, funcInputChan)

	if err := e.function.Apply(ctx, funcInputChan, e.Output); err != nil {
		if ctx.Err() != nil {
			// ignore errors caused by task cancellation
			return
		}
		e.taskError = err
	}
}

// reportStatus updates task status if failed.
func (e *TaskExecutor) reportStatus(ctx context.Context) {
	// to flush outputs before the status report
	if err := e.Output.Close(); err != nil {
		log.Error("Failed to close output: {}")
	}

	// recover panic
	taskErr := e.taskError
	if err := errorist.WrapPanic(recover()); err != nil {
		taskErr = err
	}

	if taskErr != nil {
		if err := e.taskReporter.ReportFailure(ctx, taskErr); err != nil {
			log.Error("While reporting the error, another error occurred", err)
		}
	} else if ctx.Err() == nil {
		if err := e.taskReporter.ReportSuccess(ctx); err != nil {
			log.Error("Task {} have been successfully done, but failed to report: {}", e.task.ID(), err)
		}
	}

	// to help GC
	e.function = nil
	e.Input = nil
}

func pipeAndFlattenInputs(ctx context.Context, in chan []*lrdd.Row, out chan *lrdd.Row) {
	defer close(out)

	for rows := range in {
		for _, r := range rows {
			select {
			case out <- r:
			case <-ctx.Done():
				return
			}
		}
	}
}
