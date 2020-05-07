package worker

import (
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/input"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformer"
)

type TaskExecutor struct {
	context *taskContext
	task    *job.Task

	Input    *input.Reader
	function transformer.Transformer
	Output   output.Output

	finishChan chan bool
	reporter   *job.Reporter
}

func NewTaskExecutor(c *taskContext, task *job.Task, tf transformer.Transformer, in *input.Reader, out output.Output) (*TaskExecutor, error) {
	return &TaskExecutor{
		context:    c,
		task:       task,
		Input:      in,
		function:   tf,
		Output:     out,
		reporter:   c.worker.jobReporter,
		finishChan: make(chan bool),
	}, nil
}

func (e *TaskExecutor) Run() {
	defer e.AbortOnPanic()
	rowCnt := 0
	inputChan := make(chan []*lrdd.Row)
	go func() {
		// pipe input channel
		defer close(inputChan)
		for {
			select {
			case rows, ok := <-e.Input.C:
				if e.context.Err() != nil {
					return
				}
				if !ok {
					// input is closed
					return
				}
				rowCnt += len(rows)
				inputChan <- rows

			case <-e.context.Done():
				return
			}
		}
	}()

	if err := e.function.Run(e.context, inputChan, e.Output); err != nil {
		e.Abort(err)
		return
	}
	log.Info("Task {} finished. (Total inputs {}) Closing... ", e.task.Reference(), rowCnt)

	if err := e.Output.Close(); err != nil {
		e.Abort(errors.Wrap(err, "close output"))
	}
	if err := e.reporter.ReportSuccess(e.task.Reference()); err != nil {
		log.Error("Task {} have been successfully done, but failed to report: {}", e.task.Reference(), err)
		e.Abort(errors.Wrap(err, "report successful task"))
		return
	}
	e.finishChan <- true
}

func (e *TaskExecutor) Abort(err error) {
	log.Error("Task {} failed with error: {}", e.task.Reference().String(), err)

	reportErr := e.reporter.ReportFailure(e.task.Reference(), err)
	if reportErr != nil {
		log.Error("While reporting the error, another error occurred", err)
	}
	_ = e.Output.Close()
}

func (e *TaskExecutor) AbortOnPanic() {
	if err := logger.WrapRecover(recover()); err != nil {
		e.Abort(err)
	}
}

func (e *TaskExecutor) Cancel() {
	e.context.cancel()
	if err := e.reporter.ReportCancel(e.task.Reference()); err != nil {
		log.Error("While reporting the cancellation, another error occurred", err)
	}
	_ = e.Input.Close()
	_ = e.Output.Close()
}

func (e *TaskExecutor) WaitForFinish() {
	<-e.finishChan
}
