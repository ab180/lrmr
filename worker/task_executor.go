package worker

import (
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
	defer e.AbortOnPanic()
	go e.cancelOnJobAbort()

	rowCnt := 0
	inputChan := make(chan *lrdd.Row, 100)
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
				for _, r := range rows {
					inputChan <- r
				}

			case <-e.context.Done():
				return
			}
		}
	}()

	if err := e.function.Apply(e.context, inputChan, e.Output); err != nil {
		e.Abort(err)
		return
	}
	if err := e.Output.Close(); err != nil {
		e.Abort(errors.Wrap(err, "close output"))
	}
	if err := e.reporter.ReportSuccess(e.task.Reference()); err != nil {
		log.Error("Task {} have been successfully done, but failed to report: {}", e.task.Reference(), err)
		e.Abort(errors.Wrap(err, "report successful task"))
		return
	}
	e.finishChan <- true
	e.close()
}

func (e *TaskExecutor) Abort(err error) {
	log.Error("Task {} failed with error: {}", e.task.Reference().String(), err)

	reportErr := e.reporter.ReportFailure(e.task.Reference(), err)
	if reportErr != nil {
		log.Error("While reporting the error, another error occurred", err)
	}
	_ = e.Input.Close()
	_ = e.Output.Close()
	e.close()
}

func (e *TaskExecutor) AbortOnPanic() {
	if err := logger.WrapRecover(recover()); err != nil {
		e.Abort(err)
	}
}

func (e *TaskExecutor) cancelOnJobAbort() {
	for {
		select {
		case reason := <-e.context.worker.jobManager.WatchJobErrors(e.context, e.task.JobID):
			log.Warn("Task {} canceled because job is aborted. Reason: {}", e.task.Reference(), reason)
			if err := e.reporter.ReportCancel(e.task.Reference()); err != nil {
				log.Error("While reporting the cancellation, another error occurred", err)
			}
			_ = e.Input.Close()
			_ = e.Output.Close()
			e.close()
		case <-e.context.Done():
			return
		}
	}
}

func (e *TaskExecutor) close() {
	e.context.cancel()
	e.reporter.Remove(e.task.Reference())
	e.function = nil
}

func (e *TaskExecutor) WaitForFinish() {
	<-e.finishChan
}
