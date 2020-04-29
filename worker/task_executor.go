package worker

import (
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/input"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
)

type TaskExecutor struct {
	context *taskContext
	task    *job.Task

	Input  *input.Reader
	runner stage.Runner
	Output *output.Writer

	finishChan chan bool
	reporter   *job.Reporter
}

func NewTaskExecutor(c *taskContext, task *job.Task, st stage.Stage, in *input.Reader, out *output.Writer) (*TaskExecutor, error) {
	runner, err := c.broadcast.DeserializeStage(st)
	if err != nil {
		return nil, errors.Wrapf(err, "deserialize stage %s", st.Name)
	}
	if err := runner.Setup(c); err != nil {
		return nil, errors.Wrap(err, "setup stage")
	}
	return &TaskExecutor{
		context:    c,
		task:       task,
		Input:      in,
		runner:     runner,
		Output:     out,
		reporter:   c.worker.jobReporter,
		finishChan: make(chan bool),
	}, nil
}

func (e *TaskExecutor) Run() {
	defer e.AbortOnPanic()
	rowCnt := 0
InputLoop:
	for {
		select {
		case rows, ok := <-e.Input.C:
			if e.context.Err() != nil {
				return
			}
			if !ok {
				// input is closed
				break InputLoop
			}
			rowCnt += len(rows)
			if err := e.runner.Apply(e.context, rows, e.Output); err != nil {
				e.Abort(err)
				return
			}
		case <-e.context.Done():
			return
		}
	}
	log.Info("Task {} finished. (Total inputs {}) Closing... ", e.task.Reference(), rowCnt)

	if err := e.runner.Teardown(e.context, e.Output); err != nil {
		e.Abort(errors.Wrap(err, "teardown stage"))
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
