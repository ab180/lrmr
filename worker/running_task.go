package worker

import (
	"github.com/airbloc/logger"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ivpusic/grpool"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

type runningTask struct {
	worker *Worker

	job   *job.Job
	stage *job.Stage
	task  *job.Task

	shards    *output.Shards
	executors *executors
	pool      *grpool.Pool

	connections   []lrmrpb.Worker_RunTaskServer
	finishedConns int
	isRunning     bool
	lock          sync.RWMutex

	broadcasts map[string]interface{}
}

func (t *runningTask) addConnection(c lrmrpb.Worker_RunTaskServer) {
	t.lock.Lock()
	t.connections = append(t.connections, c)
	t.lock.Unlock()
}

func (t *runningTask) finishConnection() (allFinished bool) {
	t.lock.Lock()
	defer t.lock.Unlock()
	log.Verbose("{} input finished ({}/{})", t.task.Reference(), t.finishedConns+1, len(t.connections))

	t.finishedConns += 1
	if t.finishedConns == len(t.connections) {
		allFinished = true
	}
	return
}

func (t *runningTask) createContext(partitionKey string) stage.Context {
	return &executorContext{
		runningTask:  t,
		partitionKey: partitionKey,
	}
}

func (t *runningTask) Finish() error {
	log.Info("Task {} finished. Closing... ", t.task.Reference())

	if err := t.executors.Teardown(); err != nil {
		return t.Abort(errors.Wrap(err, "finish task"))
	}
	t.pool.Release()
	go func() {
		if err := t.shards.Close(); err != nil {
			log.Error("error closing output", err)
		}
	}()

	if err := t.worker.jobReporter.ReportSuccess(t.task.Reference()); err != nil {
		log.Error("Task {} have been successfully done, but failed to report: {}", t.task.Reference(), err)
		return status.Errorf(codes.Internal, "task success report failed: %w", err)
	}
	for _, conn := range t.connections {
		_ = conn.SendAndClose(&empty.Empty{})
	}
	return nil
}
func (t *runningTask) Abort(err error) error {
	log.Error("Task {} failed with error: {}", t.task.Reference().String(), err)
	//t.pool.Release()

	reportErr := t.worker.jobReporter.ReportFailure(t.task.Reference(), err)
	if reportErr != nil {
		log.Error("While reporting the error, another error occurred", err)
		return status.Errorf(codes.Internal, "task failure report failed: %v", reportErr)
	}
	for _, conn := range t.connections {
		_ = conn.SendAndClose(&empty.Empty{})
	}
	return nil
}

func (t *runningTask) TryRecover() {
	if err := logger.WrapRecover(recover()); err != nil {
		_ = t.Abort(err)
	}
}
