package job

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/airbloc/logger"
	"github.com/therne/lrmr/cluster"
	"github.com/therne/lrmr/coordinator"
)

// JobTracker tracks and updates jobs and their tasks' status.
type Tracker struct {
	clusterState cluster.State
	jobManager   *Manager

	subscriptions map[string][]chan *Status
	subscribeLock sync.RWMutex

	activeJobs        sync.Map
	totalTasksOfStage map[string]int64
	lock              sync.RWMutex

	// stopTrack closes watcher channel.
	stopTrack context.CancelFunc

	log logger.Logger
}

func NewJobTracker(cs cluster.State, jm *Manager) *Tracker {
	return &Tracker{
		clusterState:  cs,
		jobManager:    jm,
		subscriptions: make(map[string][]chan *Status),
		log:           logger.New("lrmr.jobTracker"),
	}
}

func (t *Tracker) WaitForCompletion(jobID string) chan *Status {
	t.subscribeLock.Lock()
	defer t.subscribeLock.Unlock()

	notifyCh := make(chan *Status)
	t.subscriptions[jobID] = append(t.subscriptions[jobID], notifyCh)
	return notifyCh
}

func (t *Tracker) AddJob(job *Job) {
	t.activeJobs.Store(job.ID, job)
}

// HandleJobCompletion watches various job events such as task finish,
// and changes stage or job status.
func (t *Tracker) HandleJobCompletion() {
	wctx, cancel := context.WithCancel(context.Background())
	t.stopTrack = cancel

	t.log.Info("Start tracking...")
	for event := range t.clusterState.Watch(wctx, statusNs) {
		if strings.HasPrefix(event.Item.Key, stageStatusNs) {
			t.trackStageStatus(event)
		}
		if strings.HasPrefix(event.Item.Key, jobStatusNs) {
			t.trackJobStatus(event)
		}
	}
	t.log.Info("Stop tracking...")
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
		var st StageStatus
		if err := e.Item.Unmarshal(&st); err != nil {
			t.log.Error("Failed to unmarshal stage status on {}", err, e.Item.Key)
			return
		}
		t.log.Verbose("Stage {}/{} {}.", job.ID, stageName, st.Status)

	} else if frags[4] == "doneTasks" && e.Type == coordinator.CounterEvent {
		totalTasks := len(job.GetPartitionsOfStage(stageName))
		t.log.Info("Task ({}/{}) finished of {}/{}", e.Counter, totalTasks, job.ID, stageName)
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
		ctx, cancel := context.WithTimeout(context.TODO(), 3*time.Second)
		defer cancel()

		jobStatus, err := t.jobManager.GetJobStatus(ctx, job.ID)
		if err != nil {
			t.log.Error("Failed to get job status of {}", job.ID)
			return
		}
		if jobStatus.Status == Succeeded || jobStatus.Status == Failed {
			t.notifyJobCompletion(job, jobStatus)
		}
	}
}

func (t *Tracker) notifyJobCompletion(job *Job, jobStatus Status) {
	t.log.Info("Job {} {}. Total elapsed {}", job.ID, jobStatus.Status, time.Since(job.SubmittedAt).String())
	if jobStatus.Status == Failed {
		jobErrors, err := t.jobManager.GetJobErrors(context.TODO(), job.ID)
		if err != nil {
			t.log.Error("Failed to list error of {}", err, job.ID)
			return
		}
		for i, errDesc := range jobErrors {
			t.log.Info(" - Error #{} caused by {}: {}", i, errDesc.Task, errDesc.Message)
		}
	}
	t.subscribeLock.RLock()
	defer t.subscribeLock.RUnlock()

	for i, notifyCh := range t.subscriptions[job.ID] {
		select {
		case notifyCh <- &jobStatus:
		default:
			t.log.Verbose("Warning: subscription #{} seems too busy to receive result of job {}", i, job.ID)
		}
	}
	t.activeJobs.Delete(job.ID)
}

func (t *Tracker) CollectMetric(j *Job) (Metrics, error) {
	total := make(Metrics)
	for _, stage := range j.Stages {
		prefix := path.Join(taskStatusNs, j.ID, stage.Name)
		items, err := t.clusterState.Scan(context.TODO(), prefix)
		if err != nil {
			return nil, fmt.Errorf("scan task statuses in stage: %w", err)
		}

		metric := make(Metrics)
		for _, item := range items {
			var ts TaskStatus
			if err := item.Unmarshal(&ts); err != nil {
				return nil, fmt.Errorf("unmarshal task status of %s: %w", item.Key, err)
			}
			metric = metric.Sum(ts.Metrics)
		}
		total = total.Assign(metric.AddPrefix(stage.Name + "/"))
	}
	return total, nil
}

func (t *Tracker) Close() {
	if t.stopTrack != nil {
		t.stopTrack()
	}
}
