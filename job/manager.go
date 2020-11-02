package job

import (
	"context"
	"fmt"
	"path"

	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/cluster"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/internal/util"
	"github.com/therne/lrmr/partitions"
	"github.com/therne/lrmr/stage"
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
	clusterState cluster.State
	log          logger.Logger
}

func NewManager(cs cluster.State) *Manager {
	return &Manager{
		clusterState: cs,
		log:          logger.New("lrmr/job.Manager"),
	}
}

func (m *Manager) CreateJob(ctx context.Context, name string, stages []stage.Stage, assignments []partitions.Assignments) (*Job, error) {
	js := newStatus()
	j := &Job{
		ID:          util.GenerateID("J"),
		Name:        name,
		Stages:      stages,
		Partitions:  assignments,
		SubmittedAt: js.SubmittedAt,
	}
	txn := coordinator.NewTxn().
		Put(path.Join(jobNs, j.ID), j).
		Put(path.Join(jobStatusNs, j.ID), js)

	for _, s := range j.Stages {
		txn.Put(path.Join(stageStatusNs, j.ID, s.Name), newStageStatus())
	}
	if _, err := m.clusterState.Commit(ctx, txn); err != nil {
		return nil, errors.Wrap(err, "etcd write")
	}
	m.log.Debug("Job created: {} ({})", j.Name, j.ID)
	return j, nil
}

func (m *Manager) GetJob(ctx context.Context, jobID string) (*Job, error) {
	job := &Job{}
	if err := m.clusterState.Get(ctx, path.Join(jobNs, jobID), job); err != nil {
		return nil, err
	}
	return job, nil
}

func (m *Manager) GetJobStatus(ctx context.Context, jobID string) (Status, error) {
	var js Status
	if err := m.clusterState.Get(ctx, path.Join(jobStatusNs, jobID), &js); err != nil {
		return Status{}, err
	}
	errs, err := m.GetJobErrors(ctx, jobID)
	if err != nil {
		return Status{}, err
	}
	js.Errors = errs
	return js, nil
}

func (m *Manager) SetJobStatus(ctx context.Context, jobID string, js Status) error {
	// errors are stored in separate namespace. omit it on /job/status/:jobID
	js.Errors = nil
	return m.clusterState.Put(ctx, path.Join(jobStatusNs, jobID), &js)
}

func (m *Manager) GetJobErrors(ctx context.Context, jobID string) ([]Error, error) {
	items, err := m.clusterState.Scan(ctx, path.Join(jobErrorNs, jobID))
	if err != nil {
		return nil, err
	}
	errs := make([]Error, len(items))
	for i, item := range items {
		if err := item.Unmarshal(&errs[i]); err != nil {
			return nil, errors.Wrapf(err, "unmarshal item %s", item.Key)
		}
	}
	return errs, nil
}

func (m *Manager) WatchJobErrors(ctx context.Context, jobID string) chan Error {
	errChan := make(chan Error)
	go func() {
		for event := range m.clusterState.Watch(ctx, path.Join(jobErrorNs, jobID)) {
			var e Error
			if err := event.Item.Unmarshal(&e); err != nil {
				m.log.Error("Failed to unmarshal error desc {}: {}", err, string(event.Item.Value))
				continue
			}
			errChan <- e
		}
		close(errChan)
	}()
	return errChan
}

func (m *Manager) ListJobs(ctx context.Context, prefixFormat string, args ...interface{}) ([]*Job, error) {
	keyPrefix := path.Join(jobNs, fmt.Sprintf(prefixFormat, args...))
	results, err := m.clusterState.Scan(ctx, keyPrefix)
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

func (m *Manager) CreateTask(ctx context.Context, task *Task) (*TaskStatus, error) {
	status := NewTaskStatus()
	if err := m.clusterState.Put(ctx, path.Join(taskStatusNs, task.ID().String()), status); err != nil {
		return nil, fmt.Errorf("task write: %w", err)
	}
	return status, nil
}

func (m *Manager) GetTask(ctx context.Context, ref TaskID) (*Task, error) {
	task := &Task{}
	if err := m.clusterState.Get(ctx, path.Join(taskNs, ref.PartitionID), task); err != nil {
		return nil, errors.Wrap(err, "get task")
	}
	return task, nil
}

func (m *Manager) GetTaskStatus(ctx context.Context, ref TaskID) (*TaskStatus, error) {
	status := &TaskStatus{}
	if err := m.clusterState.Get(ctx, path.Join(taskStatusNs, ref.String()), status); err != nil {
		return nil, errors.Wrap(err, "get task")
	}
	return status, nil
}

func (m *Manager) ListTaskStatusesInJob(ctx context.Context, jobID string) ([]*TaskStatus, error) {
	items, err := m.clusterState.Scan(ctx, path.Join(taskStatusNs, jobID))
	if err != nil {
		return nil, errors.Wrap(err, "get task")
	}
	statuses := make([]*TaskStatus, len(items))
	for i, item := range items {
		statuses[i] = new(TaskStatus)
		if err := item.Unmarshal(statuses[i]); err != nil {
			return nil, errors.Wrapf(err, "unmarshal task status %s", item.Key)
		}
	}
	return statuses, nil
}
