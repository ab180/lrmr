package executor

import (
	"context"

	"github.com/ab180/lrmr/input"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/job/stage"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/lrmrpb"
	lrmrmetric "github.com/ab180/lrmr/metric"
	"github.com/ab180/lrmr/output"
	"github.com/ab180/lrmr/transformation"
	"github.com/therne/errorist"
)

type TaskExecutor struct {
	task *job.Task
	job  *runningJobHolder

	Input        *input.Reader
	Stage        *stage.Stage
	Output       *output.Writer
	OutputDesc   *lrmrpb.Output
	Metrics      lrmrmetric.Metrics
	localOptions map[string]interface{}
	taskError    error
}

func NewTaskExecutor(
	runningJob *runningJobHolder,
	task *job.Task,
	curStage *stage.Stage,
	in *input.Reader,
	outDesc *lrmrpb.Output,
	localOptions map[string]interface{},
) *TaskExecutor {
	exec := &TaskExecutor{
		task:         task,
		job:          runningJob,
		Input:        in,
		Stage:        curStage,
		OutputDesc:   outDesc,
		Metrics:      make(lrmrmetric.Metrics),
		localOptions: localOptions,
	}
	return exec
}

func (e *TaskExecutor) SetOutput(out *output.Writer) {
	e.Output = out
}

func (e *TaskExecutor) Run() {
	ctx, cancel := newTaskContextWithCancel(e.job.Context(), e)
	defer cancel()

	timer := log.Timer()
	defer timer.End("Task {} has been done", e.Stage.Name)

	defer e.reportStatus(ctx)

	// pipe input.Reader.C to function input channel
	funcInputChan := make(chan *lrdd.Row, e.Output.NumOutputs())
	go pipeAndFlattenInputs(ctx, e.Input.C, funcInputChan)

	// hard copy. TODO: a better way to do it!
	fnData, _ := e.Stage.Function.MarshalJSON()
	var function transformation.Serializable
	_ = function.UnmarshalJSON(fnData)

	if err := function.Apply(ctx, funcInputChan, e.Output); err != nil {
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
		if err := e.job.Reporter.ReportTaskFailure(ctx, e.task.ID(), taskErr, e.Metrics); err != nil {
			log.Error("While reporting the error, another error occurred", err)
		}
	} else if ctx.Err() == nil {
		if err := e.job.Reporter.ReportTaskSuccess(ctx, e.task.ID(), e.Metrics); err != nil {
			log.Error("Task {} have been successfully done, but failed to report: {}", e.task.ID(), err)
		}
	}

	// to help GC
	e.Stage = nil
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
