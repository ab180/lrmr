package worker

import (
	"context"
	"io"

	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/input"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
)

type TaskExecutor struct {
	context *taskContext
	task    *job.Task

	Input    *input.Reader
	function transformation.Transformation
	Output   output.Output

	finishChan chan bool
	reporter   *job.Reporter
}

func NewTaskExecutor(c *taskContext, task *job.Task, fn transformation.Transformation, in *input.Reader, out output.Output) (*TaskExecutor, error) {
	return &TaskExecutor{
		context:    c,
		task:       task,
		Input:      in,
		function:   fn,
		Output:     out,
		reporter:   c.worker.jobReporter,
		finishChan: make(chan bool, 1),
	}, nil
}

func (e *TaskExecutor) Run() {
	defer e.guardPanic()
	go e.cancelOnJobAbort()

	// pipe input.Reader.C to function input channel
	inputChan := make(chan *lrdd.Row, 100)
	go func() {
		defer e.guardPanic()
		defer close(inputChan)
		for rows := range e.Input.C {
			for _, r := range rows {
				if e.context.Err() != nil {
					return
				}
				inputChan <- r
			}
		}
	}()

	if err := e.function.Apply(e.context, inputChan, e.Output); err != nil {
		if errors.Cause(err) == context.Canceled || (e.context.Err() != nil && errors.Cause(err) == io.EOF) {
			// ignore errors caused by task cancellation
			return
		}
		e.Abort(err)
		return
	} else if e.context.Err() != nil {
		return
	}
	if err := e.Output.Close(); err != nil {
		e.Abort(errors.Wrap(err, "close output"))
		return
	}
	if err := e.reporter.ReportSuccess(e.task.Reference()); err != nil {
		log.Error("Task {} have been successfully done, but failed to report: {}", e.task.Reference(), err)
		e.Abort(errors.Wrap(err, "report successful task"))
		return
	}
	e.finishChan <- true
	e.context.cancel()
	e.close()
}

func (e *TaskExecutor) Abort(err error) {
	reportErr := e.reporter.ReportFailure(e.task.Reference(), err)
	if reportErr != nil {
		log.Error("While reporting the error, another error occurred", err)
	}
	e.stopExecutor()
}

func (e *TaskExecutor) guardPanic() {
	if err := logger.WrapRecover(recover()); err != nil {
		e.Abort(err)
	}
}

func (e *TaskExecutor) cancelOnJobAbort() {
	defer e.guardPanic()
	select {
	case errDesc := <-e.context.worker.jobManager.WatchJobErrors(e.context, e.task.JobID):
		log.Verbose("Task {} aborted with error caused by task {}.", e.task.Reference(), errDesc.Task)
		if err := e.reporter.ReportCancel(e.task.Reference()); err != nil {
			log.Error("While reporting the cancellation, another error occurred", err)
		}
		e.stopExecutor()
	case <-e.context.Done():
		return
	}
}

// stopExecutor stops TaskExecutor immediately.
func (e *TaskExecutor) stopExecutor() {
	e.context.cancel()
	_ = e.Output.Close()
	e.close()
}

// close frees occupied resources and memories.
func (e *TaskExecutor) close() {
	e.reporter.Remove(e.task.Reference())
	e.function = nil
}

func (e *TaskExecutor) WaitForFinish() {
	<-e.finishChan
}
