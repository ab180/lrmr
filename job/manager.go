package job

import (
	"context"
	"errors"

	lrmrmetric "github.com/ab180/lrmr/metric"
)

// ErrNotFound is returned on FetchStatus.
var ErrNotFound = errors.New("job not found")

// Manager controls a job.
type Manager interface {
	// RegisterStatus stores a job's status at the first time.
	//
	// NOTE THAT the functionality is only available on DistributedManager
	// since it's the only manager stores a job's status on the persistent storage.
	RegisterStatus(ctx context.Context) (*Status, error)

	// FetchStatus retrieves a job's current status.
	//
	// NOTE THAT the functionality is only available on DistributedManager
	// since it's the only manager stores a job's status on the persistent storage.
	FetchStatus(ctx context.Context) (*Status, error)

	// MarkTaskAsSucceed marks a task as succeed.
	// If all tasks in its belonging stage are also completed, it marks the stage as completed.
	// If all stages in belonging job are also completed, it marks the job as completed.
	MarkTaskAsSucceed(context.Context, TaskID, lrmrmetric.Metrics) error

	// MarkTaskAsFailed marks task and its belonging job as failed.
	MarkTaskAsFailed(context.Context, TaskID, error, lrmrmetric.Metrics) error

	// Abort cancels the job.
	Abort(ctx context.Context, abortedBy TaskID) error

	// OnJobCompletion registers callback for completion events of given job.
	OnJobCompletion(callback func(*Status))

	// OnStageCompletion registers callback for stage completion events in given job ID.
	OnStageCompletion(callback func(stageName string, stageStatus *StageStatus))

	// OnTaskCompletion registers callback for task completion events in given job ID.
	// For performance, only the number of currently finished tasks in its stage is given to the callback.
	OnTaskCompletion(callback func(stageName string, doneCountInStage int))

	// CollectMetrics collects task metrics in a job.
	CollectMetrics(ctx context.Context) (lrmrmetric.Metrics, error)
}
