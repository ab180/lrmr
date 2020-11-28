package job

import (
	"context"
	"strings"
	"time"

	"github.com/ab180/lrmr/coordinator"
	"github.com/airbloc/logger"
)

// JobTracker tracks and updates jobs and their tasks' status.
type Tracker struct {
	Job *Job

	jobManager   *Manager
	jobCtx       context.Context
	jobCtxCancel context.CancelFunc

	jobSubscriptions   []func(*Status)
	stageSubscriptions []func(stageName string, stageStatus *StageStatus)
	taskSubscriptions  []func(stageName string, doneCountInStage int)

	log logger.Logger
}

func newJobTracker(ctx context.Context, jm *Manager, job *Job) *Tracker {
	jobCtx, cancel := context.WithCancel(ctx)
	t := &Tracker{
		Job:          job,
		jobManager:   jm,
		jobCtx:       jobCtx,
		jobCtxCancel: cancel,
		log:          logger.New("lrmr.jobTracker"),
	}
	go t.watch()
	return t
}

// OnJobCompletion registers callback for completion events of given job.
func (t *Tracker) OnJobCompletion(callback func(*Status)) {
	t.jobSubscriptions = append(t.jobSubscriptions, callback)
}

// OnStageCompletion registers callback for stage completion events in given job ID.
func (t *Tracker) OnStageCompletion(callback func(stageName string, stageStatus *StageStatus)) {
	t.stageSubscriptions = append(t.stageSubscriptions, callback)
}

// OnTaskCompletion registers callback for task completion events in given job ID.
// For performance, only the number of currently finished tasks in its stage is given to the callback.
func (t *Tracker) OnTaskCompletion(callback func(stageName string, doneCountInStage int)) {
	t.taskSubscriptions = append(t.taskSubscriptions, callback)
}

// JobContext returns a context follows the job's lifecycle. It will be canceled after the job completion.
func (t *Tracker) JobContext() context.Context {
	return t.jobCtx
}

func (t *Tracker) watch() {
	defer t.log.Recover()

	prefix := jobStatusKey(t.Job.ID)
	for event := range t.jobManager.clusterState.Watch(t.jobCtx, prefix) {
		if strings.HasPrefix(event.Item.Key, jobStatusKey(t.Job.ID, "stage")) {
			if strings.HasSuffix(event.Item.Key, doneTasksSuffix) {
				t.handleTaskFinish(event)

			} else if strings.HasSuffix(event.Item.Key, failedTasksSuffix) {
				// TODO: handle failed tasks

			} else {
				t.handleStageStatusUpdate(event)
			}
		} else if event.Item.Key == jobStatusKey(t.Job.ID) {
			t.handleJobStatusChange()
		}
	}
}

func (t *Tracker) handleTaskFinish(e coordinator.WatchEvent) {
	stageName, err := stageNameFromStageStatusKey(e.Item.Key)
	if err != nil {
		t.log.Warn("Unable to handle task finish event: {}", err)
		return
	}
	for _, callback := range t.taskSubscriptions {
		callback(stageName, int(e.Counter))
	}
}

func (t *Tracker) handleStageStatusUpdate(e coordinator.WatchEvent) {
	stageName, err := stageNameFromStageStatusKey(e.Item.Key)
	if err != nil {
		t.log.Warn("Unable to handle stage status update event: {}", err)
		return
	}

	st := new(StageStatus)
	if err := e.Item.Unmarshal(st); err != nil {
		t.log.Error("Failed to unmarshal stage status on {}", err, e.Item.Key)
		return
	}
	for _, callback := range t.stageSubscriptions {
		callback(stageName, st)
	}
}

func (t *Tracker) handleJobStatusChange() {
	ctx, cancel := context.WithTimeout(t.jobCtx, 3*time.Second)
	defer cancel()

	jobStatus, err := t.jobManager.GetJobStatus(ctx, t.Job.ID)
	if err != nil {
		t.log.Error("Failed to get job status of {}", t.Job.ID)
		return
	}
	if jobStatus.Status == Succeeded || jobStatus.Status == Failed {
		for _, callback := range t.jobSubscriptions {
			callback(&jobStatus)
		}
		t.jobCtxCancel()
	}
}
