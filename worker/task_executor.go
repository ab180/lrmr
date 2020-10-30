package worker

import (
	"context"
	"io"

	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/input"
	"github.com/therne/lrmr/internal/serialization"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
)

type TaskExecutor struct {
	context *taskContext
	cancel  context.CancelFunc
	task    *job.Task

	Input    *input.Reader
	function transformation.Transformation
	Output   output.Output

	broadcast    serialization.Broadcast
	localOptions map[string]interface{}

	finishChan   chan struct{}
	taskReporter *job.TaskReporter
	jobManager   *job.Manager
}

func NewTaskExecutor(
	crd coordinator.Coordinator,
	j *job.Job,
	task *job.Task,
	status *job.TaskStatus,
	fn transformation.Transformation,
	in *input.Reader,
	out output.Output,
	broadcast serialization.Broadcast,
	localOptions map[string]interface{},
) *TaskExecutor {
	ctx, cancel := context.WithCancel(context.Background())
	exec := &TaskExecutor{
		task:         task,
		Input:        in,
		function:     fn,
		Output:       out,
		broadcast:    broadcast,
		localOptions: localOptions,
		finishChan:   make(chan struct{}, 1),
		taskReporter: job.NewTaskReporter(ctx, crd, j, task.ID(), status),
		jobManager:   job.NewManager(crd),
	}
	exec.context = newTaskContext(ctx, exec)
	exec.cancel = cancel
	return exec
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
	if err := e.taskReporter.ReportSuccess(); err != nil {
		log.Error("Task {} have been successfully done, but failed to report: {}", e.task.ID(), err)
	}
	e.close()
}

func (e *TaskExecutor) Abort(err error) {
	e.close()
	_ = e.Output.Close()

	reportErr := e.taskReporter.ReportFailure(err)
	if reportErr != nil {
		log.Error("While reporting the error, another error occurred", err)
	}
}

func (e *TaskExecutor) guardPanic() {
	if err := logger.WrapRecover(recover()); err != nil {
		e.Abort(err)
	}
}

func (e *TaskExecutor) cancelOnJobAbort() {
	defer e.guardPanic()
	select {
	case errDesc, ok := <-e.jobManager.WatchJobErrors(e.context, e.task.JobID):
		if !ok {
			return
		}
		log.Verbose("Task {} aborted with error caused by task {}.", e.task.ID(), errDesc.Task)
		e.Abort(nil)
	case <-e.context.Done():
		return
	}
}

// close frees occupied resources and memories.
func (e *TaskExecutor) close() {
	e.cancel()
	e.function = nil
}

func (e *TaskExecutor) WaitForFinish() {
	<-e.context.Done()
}
