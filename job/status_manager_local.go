package job

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.uber.org/atomic"
)

type LocalStatusManager struct {
	job            *Job
	jobStatus      *Status
	stageStatuses  map[string]*StageStatus
	doneStageCount atomic.Int32

	jobSubscriptions   []func(*Status)
	stageSubscriptions []func(stageName string, stageStatus *StageStatus)
	taskSubscriptions  []func(stageName string, doneCountInStage int)
	mu                 sync.RWMutex
}

func NewLocalStatusManager(j *Job) StatusManager {
	jobStatus := newStatus()
	return &LocalStatusManager{
		job:           j,
		jobStatus:     &jobStatus,
		stageStatuses: make(map[string]*StageStatus),
	}
}

func (l *LocalStatusManager) MarkTaskAsSucceed(_ context.Context, taskID TaskID) error {
	l.mu.Lock()
	defer l.mu.Unlock()

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

func (l *LocalStatusManager) markStageAsSucceed(stage string, stageStatus *StageStatus) {
	log.Verbose("Stage {} succeed", stage)

	stageStatus.Complete(Succeeded)
	for _, callback := range l.stageSubscriptions {
		go callback(stage, stageStatus)
	}

	doneStagesInJob := int(l.doneStageCount.Inc())
	if doneStagesInJob == len(l.job.Stages)-1 {
		l.markJobAsSucceed()
	}
}

func (l *LocalStatusManager) markJobAsSucceed() {
	log.Verbose("Job {} succeed", l.job.ID)

	l.jobStatus.Complete(Succeeded)
	for _, callback := range l.jobSubscriptions {
		go callback(l.jobStatus)
	}
}

func (l *LocalStatusManager) MarkTaskAsFailed(_ context.Context, causedTask TaskID, err error) error {
	log.Verbose("Job {} failed", l.job.ID)

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

func (l *LocalStatusManager) Abort(ctx context.Context, abortedBy TaskID) error {
	return l.MarkTaskAsFailed(ctx, abortedBy, errors.New("aborted"))
}

// OnJobCompletion registers callback for completion events of given job.
func (l *LocalStatusManager) OnJobCompletion(callback func(*Status)) {
	l.jobSubscriptions = append(l.jobSubscriptions, callback)
}

// OnStageCompletion registers callback for stage completion events in given job ID.
func (l *LocalStatusManager) OnStageCompletion(callback func(stageName string, stageStatus *StageStatus)) {
	l.stageSubscriptions = append(l.stageSubscriptions, callback)
}

// OnTaskCompletion registers callback for task completion events in given job ID.
// For performance, only the number of currently finished tasks in its stage is given to the callback.
func (l *LocalStatusManager) OnTaskCompletion(callback func(stageName string, doneCountInStage int)) {
	l.taskSubscriptions = append(l.taskSubscriptions, callback)
}
