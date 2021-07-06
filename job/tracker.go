package job

import (
	"context"
	"strings"
	"time"

	"github.com/ab180/lrmr/coordinator"
)

// Tracker tracks and updates jobs and their tasks' status.
type Tracker struct {
	Job *Job

	jobManager         *Manager
	jobSubscriptions   []func(*Status)
	stageSubscriptions []func(stageName string, stageStatus *StageStatus)
	taskSubscriptions  []func(stageName string, doneCountInStage int)

	shutdown chan struct{}
}

func newJobTracker(jm *Manager, job *Job) *Tracker {
	t := &Tracker{
		Job:        job,
		jobManager: jm,
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

func (t *Tracker) watch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer log.Recover()

	prefix := jobStatusKey(t.Job.ID)
	for event := range t.jobManager.clusterState.Watch(ctx, prefix) {
		if strings.HasPrefix(event.Item.Key, jobStatusKey(t.Job.ID, "stage")) {
			if strings.HasSuffix(event.Item.Key, doneTasksSuffix) {
				t.handleTaskFinish(event)

			} else if strings.HasSuffix(event.Item.Key, failedTasksSuffix) {
				// TODO: handle failed tasks

			} else {
				t.handleStageStatusUpdate(event)
			}
		} else if event.Item.Key == jobStatusKey(t.Job.ID) {
			finished := t.handleJobStatusChange()
			if finished {
				break
			}
		}
	}
}

func (t *Tracker) handleTaskFinish(e coordinator.WatchEvent) {
	stageName, err := stageNameFromStageStatusKey(e.Item.Key)
	if err != nil {
		log.Warn("Unable to handle task finish event: {}", err)
		return
	}
	for _, callback := range t.taskSubscriptions {
		go callback(stageName, int(e.Counter))
	}
}

func (t *Tracker) handleStageStatusUpdate(e coordinator.WatchEvent) {
	stageName, err := stageNameFromStageStatusKey(e.Item.Key)
	if err != nil {
		log.Warn("Unable to handle stage status update event: {}", err)
		return
	}

	st := new(StageStatus)
	if err := e.Item.Unmarshal(st); err != nil {
		log.Error("Failed to unmarshal stage status on {}", err, e.Item.Key)
		return
	}
	for _, callback := range t.stageSubscriptions {
		go callback(stageName, st)
	}
}

func (t *Tracker) handleJobStatusChange() (finished bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	jobStatus, err := t.jobManager.GetJobStatus(ctx, t.Job.ID)
	if err != nil {
		log.Error("Failed to get job status of {}", t.Job.ID)
		return false
	}
	if jobStatus.Status == Succeeded || jobStatus.Status == Failed {
		for _, callback := range t.jobSubscriptions {
			go callback(&jobStatus)
		}
		return true
	}
	return false
}

func (t *Tracker) Close() {
	t.shutdown <- struct{}{}
}
