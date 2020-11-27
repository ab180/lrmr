package job

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/coordinator"
	"github.com/airbloc/logger"
)

// JobTracker tracks and updates jobs and their tasks' status.
type Tracker struct {
	clusterState  cluster.State
	jobManager    *Manager
	subscriptions sync.Map
	activeJobs    sync.Map
	stopTrack     context.CancelFunc

	log logger.Logger
}

type subscriptionHolder struct {
	jobs   []func(*Job, *Status)
	stages []func(j *Job, stageName string, stageStatus *StageStatus)
	tasks  []func(j *Job, stageName string, doneCountInStage int)
	mu     sync.RWMutex
}

func NewJobTracker(cs cluster.State, jm *Manager) *Tracker {
	t := &Tracker{
		clusterState: cs,
		jobManager:   jm,
		log:          logger.New("lrmr.jobTracker"),
	}
	go t.watch()
	return t
}

// OnJobCompletion registers callback for completion events of given job.
func (t *Tracker) OnJobCompletion(job *Job, callback func(*Job, *Status)) {
	t.AddJob(job)
	entry, _ := t.subscriptions.LoadOrStore(job.ID, &subscriptionHolder{})
	sub := entry.(*subscriptionHolder)

	sub.mu.Lock()
	sub.jobs = append(sub.jobs, callback)
	sub.mu.Unlock()
}

// OnStageCompletion registers callback for stage completion events in given job ID.
func (t *Tracker) OnStageCompletion(job *Job, callback func(j *Job, stageName string, stageStatus *StageStatus)) {
	t.AddJob(job)
	entry, _ := t.subscriptions.LoadOrStore(job.ID, &subscriptionHolder{})
	sub := entry.(*subscriptionHolder)

	sub.mu.Lock()
	sub.stages = append(sub.stages, callback)
	sub.mu.Unlock()
}

// OnTaskCompletion registers callback for task completion events in given job ID.
// For performance, only the number of currently finished tasks in its stage is given to the callback.
func (t *Tracker) OnTaskCompletion(job *Job, callback func(job *Job, stageName string, doneCountInStage int)) {
	t.AddJob(job)
	entry, _ := t.subscriptions.LoadOrStore(job.ID, &subscriptionHolder{})
	sub := entry.(*subscriptionHolder)

	sub.mu.Lock()
	sub.tasks = append(sub.tasks, callback)
	sub.mu.Unlock()
}

func (t *Tracker) AddJob(job *Job) {
	t.activeJobs.Store(job.ID, job)
}

func (t *Tracker) watch() {
	defer t.log.Recover()

	wctx, cancel := context.WithCancel(context.Background())
	t.stopTrack = cancel

	for event := range t.clusterState.Watch(wctx, statusNs) {
		if strings.HasPrefix(event.Item.Key, stageStatusNs) {
			t.trackStageStatus(event)
		}
		if strings.HasPrefix(event.Item.Key, jobStatusNs) {
			t.trackJobStatus(event)
		}
	}
}

func (t *Tracker) trackStageStatus(e coordinator.WatchEvent) {
	frags := strings.Split(e.Item.Key, "/")
	if len(frags) < 4 {
		t.log.Warn("Found unknown stage status: {}", e.Item.Key)
		return
	}
	j, ok := t.activeJobs.Load(frags[2])
	if !ok {
		return
	}
	job := j.(*Job)
	stageName := frags[3]

	if len(frags) == 4 {
		// stage status update
		st := new(StageStatus)
		if err := e.Item.Unmarshal(st); err != nil {
			t.log.Error("Failed to unmarshal stage status on {}", err, e.Item.Key)
			return
		}
		sub, release := t.getSubscription(job.ID)
		defer release()

		for _, callback := range sub.stages {
			go callback(job, stageName, st)
		}

	} else if frags[4] == "doneTasks" && e.Type == coordinator.CounterEvent {
		sub, release := t.getSubscription(job.ID)
		defer release()

		for _, callback := range sub.tasks {
			go callback(job, stageName, int(e.Counter))
		}
	}
}

func (t *Tracker) trackJobStatus(e coordinator.WatchEvent) {
	frags := strings.Split(e.Item.Key, "/")
	if len(frags) < 3 {
		t.log.Warn("Found unknown job status: {}", e.Item.Key)
		return
	}
	j, ok := t.activeJobs.Load(frags[2])
	if !ok {
		return
	}
	job := j.(*Job)

	if len(frags) == 3 && e.Type == coordinator.PutEvent {
		// job status update
		ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
		defer cancel()

		jobStatus, err := t.jobManager.GetJobStatus(ctx, job.ID)
		if err != nil {
			t.log.Error("Failed to get job status of {}", job.ID)
			return
		}
		if jobStatus.Status == Succeeded || jobStatus.Status == Failed {
			sub, release := t.getSubscription(job.ID)
			defer release()

			for _, callback := range sub.jobs {
				go callback(job, &jobStatus)
			}
			t.activeJobs.Delete(job.ID)
		}
	}
}

func (t *Tracker) getSubscription(jobID string) (sub *subscriptionHolder, release func()) {
	entry, ok := t.subscriptions.Load(jobID)
	if !ok {
		return
	}
	sub = entry.(*subscriptionHolder)

	sub.mu.RLock()
	release = func() {
		sub.mu.RUnlock()
		if err := logger.WrapRecover(recover()); err != nil {
			t.log.Error("Panic occurred during the calling of callbacks for job {}", err, jobID)
		}
	}
	return sub, release
}

func (t *Tracker) Close() {
	t.stopTrack()
}
