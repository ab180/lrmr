package job

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/internal/util"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/stage"

	"path"
)

const (
	jobNs         = "jobs/"
	taskNs        = "tasks/"
	statusNs      = "status/"
	nodeStatusNs  = "status/node/"
	stageStatusNs = "status/stages/"
	taskStatusNs  = "status/tasks/"
	jobStatusNs   = "status/jobs"
	jobErrorNs    = "errors/jobs"
)

type Manager struct {
	nodeManager node.Manager
	crd         coordinator.Coordinator
	log         logger.Logger
}

func NewManager(nm node.Manager, crd coordinator.Coordinator) *Manager {
	return &Manager{
		nodeManager: nm,
		crd:         crd,
		log:         logger.New("jobmanager"),
	}
}

func (m *Manager) CreateJob(ctx context.Context, name string, stages []stage.Stage) (*Job, error) {
	js := newStatus()
	j := &Job{
		ID:          util.GenerateID("J"),
		Name:        name,
		Stages:      stages,
		SubmittedAt: js.SubmittedAt,
	}
	txn := coordinator.NewTxn().
		// Put(path.Join(jobNs, j.ID), j).
		Put(path.Join(jobStatusNs, j.ID), js)

	for _, s := range j.Stages {
		txn.Put(path.Join(stageStatusNs, j.ID, s.Name), newStageStatus())
	}
	if err := m.crd.Commit(ctx, txn); err != nil {
		return nil, errors.Wrap(err, "etcd write")
	}
	m.log.Debug("Job created: {} ({})", j.Name, j.ID)
	return j, nil
}

func (m *Manager) GetJob(ctx context.Context, jobID string) (*Job, error) {
	job := &Job{}
	if err := m.crd.Get(ctx, path.Join(jobNs, jobID), job); err != nil {
		return nil, err
	}
	return job, nil
}

func (m *Manager) GetJobStatus(ctx context.Context, jobID string) (*Status, error) {
	job := new(Status)
	if err := m.crd.Get(ctx, path.Join(jobStatusNs, jobID), job); err != nil {
		return nil, err
	}
	return job, nil
}

func (m *Manager) GetJobErrors(ctx context.Context, jobID string) (stacktraces []string, err error) {
	items, err := m.crd.Scan(ctx, path.Join(jobErrorNs, jobID))
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		var s string
		if err := item.Unmarshal(&s); err != nil {
			return nil, errors.Wrapf(err, "unmarshal item %s", item.Key)
		}
		stacktraces = append(stacktraces, s)
	}
	return stacktraces, nil
}

func (m *Manager) WatchJobErrors(ctx context.Context, jobID string) chan string {
	stacktraceChan := make(chan string)
	go func() {
		for event := range m.crd.Watch(ctx, path.Join(jobErrorNs, jobID)) {
			var s string
			if err := event.Item.Unmarshal(&s); err != nil {
				m.log.Error("Failed to unmarshal stacktrace {}: {}", err, string(event.Item.Value))
			}
			stacktraceChan <- s
		}
	}()
	return stacktraceChan
}

func (m *Manager) ListJobs(ctx context.Context, prefixFormat string, args ...interface{}) ([]*Job, error) {
	keyPrefix := path.Join(jobNs, fmt.Sprintf(prefixFormat, args...))
	results, err := m.crd.Scan(ctx, keyPrefix)
	if err != nil {
		return nil, err
	}
	jobs := make([]*Job, len(results))
	for i, item := range results {
		job := &Job{}
		if err := item.Unmarshal(job); err != nil {
			return nil, fmt.Errorf("unmarshal %s: %w", item.Key, err)
		}
		jobs[i] = job
	}
	return jobs, nil
}

func (m *Manager) ListTasks(ctx context.Context, prefixFormat string, args ...interface{}) ([]*Task, error) {
	keyPrefix := path.Join(taskNs, fmt.Sprintf(prefixFormat, args...))
	results, err := m.crd.Scan(ctx, keyPrefix)
	if err != nil {
		return nil, err
	}
	tasks := make([]*Task, len(results))
	for i, item := range results {
		task := &Task{}
		if err := item.Unmarshal(task); err != nil {
			return nil, fmt.Errorf("unmarshal %s: %w", item.Key, err)
		}
		tasks[i] = task
	}
	return tasks, nil
}

func (m *Manager) CreateTask(ctx context.Context, task *Task) (*TaskStatus, error) {
	status := newTaskStatus()
	txn := coordinator.NewTxn().
		// Put(path.Join(taskNs, task.ID()), task).
		Put(path.Join(taskStatusNs, task.Reference().String()), status).
		IncrementCounter(path.Join(stageStatusNs, task.JobID, task.StageName, "totalTasks")).
		IncrementCounter(path.Join(nodeStatusNs, task.NodeID, "totalTasks"))

	if err := m.crd.Commit(ctx, txn); err != nil {
		return nil, fmt.Errorf("task write: %w", err)
	}
	return status, nil
}

func (m *Manager) GetTask(ctx context.Context, ref TaskReference) (*Task, error) {
	task := &Task{}
	if err := m.crd.Get(ctx, path.Join(taskNs, ref.TaskID), task); err != nil {
		return nil, errors.Wrap(err, "get task")
	}
	return task, nil
}

func (m *Manager) GetTaskStatus(ctx context.Context, ref TaskReference) (*TaskStatus, error) {
	status := &TaskStatus{}
	if err := m.crd.Get(ctx, path.Join(taskStatusNs, ref.String()), status); err != nil {
		return nil, errors.Wrap(err, "get task")
	}
	return status, nil
}
