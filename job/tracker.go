package job

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/coordinator"
)

// JobTracker tracks and updates jobs and their tasks' status.
type Tracker struct {
	crd        coordinator.Coordinator
	jobManager *Manager

	subscriptions map[string][]chan *Status

	activeJobs        map[string]*Job
	totalTasksOfStage map[string]int64
	lock              sync.RWMutex

	// stopTrack closes watcher channel.
	stopTrack context.CancelFunc

	log logger.Logger
}

func NewJobTracker(crd coordinator.Coordinator, jm *Manager) *Tracker {
	return &Tracker{
		crd:               crd,
		jobManager:        jm,
		subscriptions:     make(map[string][]chan *Status),
		activeJobs:        make(map[string]*Job),
		totalTasksOfStage: make(map[string]int64),
		log:               logger.New("lrmr/job.Tracker"),
	}
}

func (t *Tracker) WaitForCompletion(jobID string) chan *Status {
	notifyCh := make(chan *Status)
	t.subscriptions[jobID] = append(t.subscriptions[jobID], notifyCh)
	return notifyCh
}

func (t *Tracker) AddJob(job *Job) {
	t.lock.Lock()
	t.activeJobs[job.ID] = job
	t.lock.Unlock()
}

// HandleJobCompletion watches various job events such as task finish,
// and changes stage or job status.
func (t *Tracker) HandleJobCompletion() {
	wctx, cancel := context.WithCancel(context.Background())
	t.stopTrack = cancel

	t.log.Info("Start tracking...")
	wc := t.crd.Watch(wctx, statusNs)
	for event := range wc {
		if strings.HasPrefix(event.Item.Key, stageStatusNs) {
			t.trackStage(event)
		}
	}
	t.log.Info("Stop tracking...")
}

func (t *Tracker) trackStage(e coordinator.WatchEvent) {
	frags := strings.Split(e.Item.Key, "/")
	if len(frags) < 4 {
		t.log.Warn("Found unknown stage status: {}", e.Item.Key)
		return
	}
	job, ok := t.activeJobs[frags[2]]
	if !ok {
		return
	}
	stageID := frags[2] + "/" + frags[3]

	if strings.HasSuffix(e.Item.Key, "totalTasks") && e.Type == coordinator.CounterEvent {
		// just increase because we can't ensure the order of the events
		t.lock.Lock()
		t.totalTasksOfStage[stageID] += 1
		t.lock.Unlock()

	} else if strings.HasSuffix(e.Item.Key, "doneTasks") && e.Type == coordinator.CounterEvent {
		t.lock.RLock()
		totalTasks := t.totalTasksOfStage[stageID]
		t.lock.RUnlock()

		// t.log.Info("Task ({}/{}) finished of {}", e.Counter, totalTasks, stageID)
		if e.Counter == totalTasks {
			err := t.finalizeStage(context.TODO(), job, stageID)
			if err != nil {
				t.log.Error("Failed to finalize stage", err)
				return
			}
		}
	}
}

func (t *Tracker) finalizeStage(ctx context.Context, job *Job, stageID string) error {
	var s StageStatus
	key := path.Join(stageStatusNs, stageID)
	if err := t.crd.Get(context.TODO(), key, &s); err != nil {
		return fmt.Errorf("read stage status: %w", err)
	}
	failedTasks, err := t.crd.ReadCounter(ctx, path.Join(key, "failedTasks"))
	if err != nil {
		return fmt.Errorf("read failed task counts: %w", err)
	}

	if failedTasks > 0 {
		s.Complete(Failed)
	} else {
		s.Complete(Succeeded)
	}
	if err := t.crd.Put(ctx, key, s); err != nil {
		return fmt.Errorf("update stage status: %w", err)
	}

	doneStagesKey := path.Join(jobStatusNs, job.ID, "doneStages")
	doneStages, err := t.crd.IncrementCounter(ctx, doneStagesKey)
	if err != nil {
		return fmt.Errorf("increment done stage count: %w", err)
	}
	t.lock.Lock()
	delete(t.totalTasksOfStage, stageID)
	t.lock.Unlock()

	totalStages := int64(len(job.Stages)) - 2
	t.log.Verbose("Stage {} {}. ({}/{})", stageID, s.Status, doneStages, totalStages)

	if s.Status == Failed {
		return t.finalizeJob(ctx, job, Failed)

	} else if doneStages == totalStages {
		return t.finalizeJob(ctx, job, Succeeded)
	}
	return nil
}

func (t *Tracker) finalizeJob(ctx context.Context, job *Job, s RunningState) error {
	js, err := t.jobManager.GetJobStatus(ctx, job.ID)
	if err != nil {
		return errors.Wrapf(err, "get status of job %s", job.ID)
	}
	js.Complete(s)
	if err := t.jobManager.SetJobStatus(ctx, job.ID, js); err != nil {
		return errors.Wrapf(err, "update status of job %s", job.ID)
	}
	t.log.Info("Job {} {}. Total elapsed {}", job.ID, s, time.Since(job.SubmittedAt).String())
	for i, notifyCh := range t.subscriptions[job.ID] {
		select {
		case notifyCh <- &js:
		default:
			t.log.Verbose("Warning: subscription #{} seems too busy to receive result of job {}", i, job.ID)
		}
	}
	delete(t.activeJobs, job.ID)
	return nil
}

func (t *Tracker) CollectMetric(j *Job) (Metrics, error) {
	total := make(Metrics)
	for _, stage := range j.Stages {
		prefix := path.Join(taskStatusNs, j.ID, stage.Name)
		items, err := t.crd.Scan(context.TODO(), prefix)
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
