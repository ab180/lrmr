package executor

import (
	"context"
	"errors"

	"github.com/ab180/lrmr/input"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/job/stage"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/lrmrpb"
	lrmrmetric "github.com/ab180/lrmr/metric"
	"github.com/ab180/lrmr/output"
	"github.com/ab180/lrmr/transformation"
	"github.com/rs/zerolog/log"
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
	opt          *Options
	taskError    error
}

func NewTaskExecutor(
	runningJob *runningJobHolder,
	task *job.Task,
	curStage *stage.Stage,
	in *input.Reader,
	outDesc *lrmrpb.Output,
	localOptions map[string]interface{},
	opt *Options,
) *TaskExecutor {
	exec := &TaskExecutor{
		task:         task,
		job:          runningJob,
		Input:        in,
		Stage:        curStage,
		OutputDesc:   outDesc,
		Metrics:      make(lrmrmetric.Metrics),
		localOptions: localOptions,
		opt:          opt,
	}
	return exec
}

func (e *TaskExecutor) SetOutput(out *output.Writer) {
	e.Output = out
}

func (e *TaskExecutor) Run() {
	ctx, cancel := newTaskContextWithCancel(e.job.Context(), e)
	defer cancel()

	defer e.reportStatus(ctx)

	// pipe input.Reader.C to function input channel
	funcInputChan := make(chan []lrdd.Row, e.Output.NumOutputs())
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
	var errs error

	// to flush outputs before the status report
	if err := e.Output.Close(); err != nil {
		errs = errors.Join(errs, err)
	}

	if e.taskError != nil {
		errs = errors.Join(errs, e.taskError)
	}

	// recover panic
	if err := errorist.WrapPanic(recover()); err != nil {
		errs = errors.Join(errs, err)
	}

	if errs != nil {
		if err := e.job.Reporter.ReportTaskFailure(ctx, e.task.ID(), errs, e.Metrics); err != nil {
			log.Warn().
				Err(err).
				Msg("While reporting the error, another error occurred")
		}
	} else if ctx.Err() == nil {
		if err := e.job.Reporter.ReportTaskSuccess(ctx, e.task.ID(), e.Metrics); err != nil {
			log.Warn().
				Err(err).
				Interface("taskID", e.task.ID()).
				Msg("Task have been successfully done, but failed to report")
		}
	}

	// to help GC
	e.Stage = nil
	e.Input = nil
}

func pipeAndFlattenInputs(ctx context.Context, in chan []lrdd.Row, out chan []lrdd.Row) {
	defer close(out)

	for rows := range in {
		select {
		case out <- rows:
		case <-ctx.Done():
			return
		}
	}
}
