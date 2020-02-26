package node

import (
	"context"
	"fmt"
	"github.com/therne/lrmr/coordinator"
	"path"
	"time"
)

const (
	jobNs  = "jobs/"
	taskNs = "tasks/"

	statusNs      = "status/"
	stageStatusNs = "status/stages/"
	taskStatusNs  = "status/tasks/"
	jobStatusNs   = "status/jobs"
)

type JobManager interface {
	CreateJob(ctx context.Context, name string, stages []*Stage) (*Job, error)
	GetJob(ctx context.Context, jobID string) (*Job, error)

	ListJobs(ctx context.Context, prefixFormat string, args ...interface{}) ([]*Job, error)
	ListTasks(ctx context.Context, prefixFormat string, args ...interface{}) ([]*Task, error)

	CreateTask(ctx context.Context, task *Task) error
	UpdateTaskStatus(taskRef string, new Status) error
	ReportTaskSuccess(ref TaskReference) error
	ReportTaskFailure(ref TaskReference, err error) error
}

func (m *manager) CreateJob(ctx context.Context, name string, stages []*Stage) (*Job, error) {
	allNodes, err := m.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}
	js := &JobStatus{Status: Starting}
	j := &Job{
		ID:          mustGenerateID("j"),
		Name:        name,
		Workers:     allNodes,
		Stages:      stages,
		SubmittedAt: time.Now(),
	}
	for _, stage := range j.Stages {
		if stage.Name == "__input" || stage.Name == "__collect" {
			// input / collector stage runs on the master
			stage.Workers = []*Node{{ID: "master", Host: "localhost"}}
		} else {
			// TODO: resource-based scheduling
			stage.Workers = allNodes
		}
	}

	writes := []coordinator.BatchOp{
		coordinator.Put(path.Join(jobNs, j.ID), j),
		coordinator.Put(path.Join(jobStatusNs, j.ID), js),
	}
	for _, stage := range j.Stages {
		statKey := path.Join(stageStatusNs, j.ID, stage.Name)
		statVal := &StageStatus{
			Status:      Starting,
			SubmittedAt: now(),
		}
		writes = append(writes, coordinator.Put(statKey, statVal))
	}
	if err := m.crd.Batch(ctx, writes...); err != nil {
		return nil, fmt.Errorf("etcd write: %w", err)
	}
	return j, nil
}

func (m *manager) GetJob(ctx context.Context, jobID string) (*Job, error) {
	job := &Job{}
	if err := m.crd.Get(ctx, path.Join(jobNs, jobID), job); err != nil {
		return nil, err
	}
	return job, nil
}

func (m *manager) ListJobs(ctx context.Context, prefixFormat string, args ...interface{}) ([]*Job, error) {
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

func (m *manager) ListTasks(ctx context.Context, prefixFormat string, args ...interface{}) ([]*Task, error) {
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

func (m *manager) CreateTask(ctx context.Context, task *Task) error {
	if err := m.crd.Put(ctx, path.Join(taskNs, task.ID), task); err != nil {
		return fmt.Errorf("task creation: %w", err)
	}
	status := &TaskStatus{
		Status:      Starting,
		SubmittedAt: &task.SubmittedAt,
	}
	ops := []coordinator.BatchOp{
		coordinator.Put(path.Join(taskStatusNs, task.Reference().String()), status),
		coordinator.IncrementCounter(path.Join(stageStatusNs, task.JobID, task.StageName, "totalTasks")),
	}
	return m.crd.Batch(ctx, ops...)
}

func (m *manager) UpdateTaskStatus(taskRef string, new Status) error {
	key := path.Join(taskStatusNs, taskRef)
	var status TaskStatus
	if err := m.crd.Get(context.Background(), key, &status); err != nil {
		return fmt.Errorf("read task status: %s", err)
	}
	old := status.Status

	status.Status = new
	if err := m.crd.Put(context.Background(), key, &status); err != nil {
		return fmt.Errorf("update task status: %s", err)
	}
	m.log.Info("Task {} updated to {} -> {}", taskRef, old, new)
	return nil
}

func (m *manager) ReportTaskSuccess(ref TaskReference) error {
	ctx := context.TODO()

	var status TaskStatus
	key := path.Join(taskStatusNs, ref.String())
	if err := m.crd.Get(context.Background(), key, &status); err != nil {
		return fmt.Errorf("read task status: %s", err)
	}
	status.Complete(Succeeded)

	elapsed := status.CompletedAt.Sub(*status.SubmittedAt)
	m.log.Info("Task {} succeeded after {}", ref.String(), elapsed.String())

	if err := m.crd.Put(ctx, key, &status); err != nil {
		return fmt.Errorf("report successful task: %w", err)
	}
	if _, err := m.crd.IncrementCounter(ctx, stageStatusKey(ref, "doneTasks")); err != nil {
		return fmt.Errorf("increase done task count: %w", err)
	}
	return nil
}

func (m *manager) ReportTaskFailure(ref TaskReference, err error) error {
	ctx := context.TODO()
	var status TaskStatus

	key := path.Join(taskStatusNs, ref.String())
	if err := m.crd.Get(ctx, key, &status); err != nil {
		return fmt.Errorf("read task status: %s", err)
	}
	status.Complete(Failed)
	status.Error = err.Error()

	if err := m.crd.Put(context.Background(), key, &status); err != nil {
		return fmt.Errorf("report failed task: %w", err)
	}

	elapsed := status.CompletedAt.Sub(*status.SubmittedAt)
	m.log.Error("Task {} failed after {} with error: {}", ref.String(), elapsed.String(), err.Error())

	return m.crd.Batch(
		ctx,
		coordinator.IncrementCounter(stageStatusKey(ref, "doneTasks")),
		coordinator.IncrementCounter(stageStatusKey(ref, "failedTasks")),
	)
}

// stageStatusKey returns a key of stage summary entry with given name.
func stageStatusKey(ref TaskReference, name string) string {
	return path.Join(stageStatusNs, ref.JobID, ref.StageName, name)
}

// jobStatusKey returns a key of job summary entry with given name.
func jobStatusKey(ref TaskReference, name string) string {
	return path.Join(jobStatusNs, ref.JobID, name)
}
