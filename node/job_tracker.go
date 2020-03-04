package node

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/coordinator"
	"path"
	"strings"
	"sync"
	"time"
)

// JobTracker tracks and updates jobs and their tasks' status.
type JobTracker struct {
	crd coordinator.Coordinator
	log logger.Logger

	subscriptions map[string][]chan *JobStatus

	activeJobs        map[string]*Job
	totalTasksOfStage map[string]int64
	lock              sync.RWMutex

	// stopTrack closes watcher channel.
	stopTrack context.CancelFunc
}

func NewJobTracker(crd coordinator.Coordinator) *JobTracker {
	return &JobTracker{
		crd:               crd,
		log:               logger.New("jobtracker"),
		subscriptions:     make(map[string][]chan *JobStatus),
		activeJobs:        make(map[string]*Job),
		totalTasksOfStage: make(map[string]int64),
	}
}

func (jt *JobTracker) WaitForCompletion(jobID string) chan *JobStatus {
	notifyCh := make(chan *JobStatus)
	jt.subscriptions[jobID] = append(jt.subscriptions[jobID], notifyCh)
	return notifyCh
}

func (jt *JobTracker) Start() {
	wctx, cancel := context.WithCancel(context.Background())
	jt.stopTrack = cancel

	wc := jt.crd.Watch(wctx, statusNs)
	go jt.doTrack(wc)
}

func (jt *JobTracker) AddJob(job *Job) {
	jt.lock.Lock()
	jt.activeJobs[job.ID] = job
	jt.lock.Unlock()
}

func (jt *JobTracker) doTrack(wc chan coordinator.WatchEvent) {
	jt.log.Info("Start tracking...")
	for event := range wc {
		if strings.HasPrefix(event.Item.Key, stageStatusNs) {
			jt.trackStage(event)
		}
	}
	jt.log.Info("Stop tracking...")
}

func (jt *JobTracker) trackStage(e coordinator.WatchEvent) {
	frags := strings.Split(e.Item.Key, "/")
	if len(frags) < 4 {
		jt.log.Warn("Found unknown stage status: {}", e.Item.Key)
		return
	}
	job, ok := jt.activeJobs[frags[2]]
	if !ok {
		return
	}
	stageID := frags[2] + "/" + frags[3]

	if strings.HasSuffix(e.Item.Key, "totalTasks") && e.Type == coordinator.CounterEvent {
		// just increase because we can't ensure the order of the events
		jt.lock.Lock()
		jt.totalTasksOfStage[stageID] += 1
		jt.lock.Unlock()

	} else if strings.HasSuffix(e.Item.Key, "doneTasks") && e.Type == coordinator.CounterEvent {
		jt.lock.RLock()
		totalTasks := jt.totalTasksOfStage[stageID]
		jt.lock.RUnlock()

		jt.log.Info("Task ({}/{}) finished of {}", e.Counter, totalTasks, stageID)
		if e.Counter == totalTasks {
			err := jt.finalizeStage(context.TODO(), job, stageID)
			if err != nil {
				jt.log.Error("Failed to finalize stage", err)
				return
			}
		}
	}
}

func (jt *JobTracker) finalizeStage(ctx context.Context, job *Job, stageID string) error {
	var s StageStatus
	key := path.Join(stageStatusNs, stageID)
	if err := jt.crd.Get(context.TODO(), key, &s); err != nil {
		return fmt.Errorf("read stage status: %w", err)
	}
	failedTasks, err := jt.crd.ReadCounter(ctx, path.Join(key, "failedTasks"))
	if err != nil {
		return fmt.Errorf("read failed task counts: %w", err)
	}

	if failedTasks > 0 {
		s.Complete(Failed)
	} else {
		s.Complete(Succeeded)
	}
	if err := jt.crd.Put(ctx, key, s); err != nil {
		return fmt.Errorf("update stage status: %w", err)
	}

	doneStagesKey := path.Join(jobStatusNs, job.ID, "doneStages")
	doneStages, err := jt.crd.IncrementCounter(ctx, doneStagesKey)
	if err != nil {
		return fmt.Errorf("increment done stage count: %w", err)
	}
	jt.lock.Lock()
	delete(jt.totalTasksOfStage, stageID)
	jt.lock.Unlock()

	// exclude input stage
	totalStages := int64(len(job.Stages) - 1)
	jt.log.Info("Stage {} {}. ({}/{})", stageID, s.Status, doneStages, totalStages)

	if s.Status == Failed {
		return jt.finalizeJob(ctx, job, Failed)

	} else if doneStages == totalStages {
		return jt.finalizeJob(ctx, job, Succeeded)
	}
	return nil
}

func (jt *JobTracker) finalizeJob(ctx context.Context, job *Job, s Status) error {
	var js JobStatus
	key := path.Join(jobStatusNs, job.ID)
	if err := jt.crd.Get(context.TODO(), key, &js); err != nil {
		return fmt.Errorf("read job status: %w", err)
	}
	js.Complete(s)
	if err := jt.crd.Put(ctx, key, js); err != nil {
		return fmt.Errorf("update job status: %w", err)
	}
	jt.log.Info("Job {} {}. Total elapsed {}", job.ID, s, time.Since(job.SubmittedAt).String())

	// print metrics collected from the stage
	m, err := jt.collectMetric(job)
	if err != nil {
		jt.log.Warn("failed to collect metric: {}", err)
	}
	jt.log.Info("{} metrics have been collected.", len(m))
	for k, v := range m {
		jt.log.Info("    {} = {}", k, v)
	}

	for _, notifyCh := range jt.subscriptions[job.ID] {
		notifyCh <- &js
	}
	delete(jt.activeJobs, job.ID)
	return nil
}

func (jt *JobTracker) collectMetric(j *Job) (Metrics, error) {
	total := make(Metrics)
	for _, stage := range j.Stages {
		prefix := path.Join(taskStatusNs, j.ID, stage.Name)
		items, err := jt.crd.Scan(context.TODO(), prefix)
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

func (jt *JobTracker) Close() {
	if jt.stopTrack != nil {
		jt.stopTrack()
	}
}
