package job

import (
	"context"
	"errors"
	"fmt"
	"sync"

	lrmrmetric "github.com/ab180/lrmr/metric"
	"github.com/rs/zerolog/log"
	"go.uber.org/atomic"
)

type LocalManager struct {
	job            *Job
	jobStatus      *Status
	stageStatuses  map[string]*StageStatus
	doneStageCount atomic.Int32
	metrics        lrmrmetric.Metrics

	jobSubscriptions   []func(*Status)
	stageSubscriptions []func(stageName string, stageStatus *StageStatus)
	taskSubscriptions  []func(stageName string, doneCountInStage int)
	mu                 sync.RWMutex
}

func NewLocalManager(j *Job) Manager {
	return &LocalManager{
		job:           j,
		jobStatus:     newStatus(),
		stageStatuses: make(map[string]*StageStatus),
		metrics:       make(lrmrmetric.Metrics),
	}
}

func (l *LocalManager) RegisterStatus(ctx context.Context) (*Status, error) {
	panic("not supported")
}

func (l *LocalManager) FetchStatus(ctx context.Context) (*Status, error) {
	return l.jobStatus, nil
}

func (l *LocalManager) MarkTaskAsSucceed(_ context.Context, taskID TaskID, metrics lrmrmetric.Metrics) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.mergeTaskMetricsIntoJobMetrics(metrics)

	belongingStage, ok := l.stageStatuses[taskID.StageName]
	if !ok {
		belongingStage = newStageStatus()
		l.stageStatuses[taskID.StageName] = belongingStage
	}
	doneTasksInStage := int(belongingStage.DoneTasks.Inc())
	for _, callback := range l.taskSubscriptions {
		go callback(taskID.StageName, doneTasksInStage)
	}

	if doneTasksInStage == len(l.job.GetPartitionsOfStage(taskID.StageName)) {
		l.markStageAsSucceed(taskID.StageName, belongingStage)
	}
	return nil
}

func (l *LocalManager) markStageAsSucceed(stage string, stageStatus *StageStatus) {
	log.Debug().
		Str("job_id", l.job.ID).
		Str("stage", stage).
		Msg("stage succeed")

	stageStatus.Complete(Succeeded)
	for _, callback := range l.stageSubscriptions {
		go callback(stage, stageStatus)
	}

	doneStagesInJob := int(l.doneStageCount.Inc())
	if doneStagesInJob == len(l.job.Stages)-1 {
		l.markJobAsSucceed()
	}
}

func (l *LocalManager) markJobAsSucceed() {
	log.Debug().
		Str("job_id", l.job.ID).
		Msg("job succeed")

	l.jobStatus.Complete(Succeeded)
	for _, callback := range l.jobSubscriptions {
		go callback(l.jobStatus)
	}
}

func (l *LocalManager) MarkTaskAsFailed(_ context.Context, causedTask TaskID, err error, metrics lrmrmetric.Metrics) error { //nolint:lll
	l.mu.Lock()
	defer l.mu.Unlock()

	l.mergeTaskMetricsIntoJobMetrics(metrics)
	log.Debug().
		Str("job_id", l.job.ID).
		Msg("job failed")

	l.jobStatus.Complete(Failed)
	l.jobStatus.Errors = append(l.jobStatus.Errors, Error{
		Task:       causedTask.String(),
		Message:    err.Error(),
		Stacktrace: fmt.Sprintf("%+v", err),
	})
	for _, callback := range l.jobSubscriptions {
		go callback(l.jobStatus)
	}
	return nil
}

func (l *LocalManager) Abort(ctx context.Context, abortedBy TaskID) error {
	return l.MarkTaskAsFailed(ctx, abortedBy, errors.New("aborted"), nil)
}

// OnJobCompletion registers callback for completion events of given job.
func (l *LocalManager) OnJobCompletion(callback func(*Status)) {
	l.jobSubscriptions = append(l.jobSubscriptions, callback)
}

// OnStageCompletion registers callback for stage completion events in given job ID.
func (l *LocalManager) OnStageCompletion(callback func(stageName string, stageStatus *StageStatus)) {
	l.stageSubscriptions = append(l.stageSubscriptions, callback)
}

// OnTaskCompletion registers callback for task completion events in given job ID.
// For performance, only the number of currently finished tasks in its stage is given to the callback.
func (l *LocalManager) OnTaskCompletion(callback func(stageName string, doneCountInStage int)) {
	l.taskSubscriptions = append(l.taskSubscriptions, callback)
}

func (l *LocalManager) mergeTaskMetricsIntoJobMetrics(metrics lrmrmetric.Metrics) {
	l.metrics.Add(metrics)
}

func (l *LocalManager) CollectMetrics(context.Context) (lrmrmetric.Metrics, error) {
	return l.metrics, nil
}

func (*LocalManager) Close() {}
