package node

import (
	"fmt"
	"time"
)

type Job struct {
	ID   string `json:"id"`
	Name string `json:"name"`

	Stages  []*Stage `json:"stages"`
	Workers []*Node  `json:"workers"`

	SubmittedAt time.Time `json:"submittedAt"`
}

func (j *Job) GetStage(name string) *Stage {
	for _, stage := range j.Stages {
		if stage.Name == name {
			return stage
		}
	}
	return nil
}

type JobStatus struct {
	Status      Status     `json:"status"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

func (js *JobStatus) Complete(s Status) {
	now := time.Now()
	js.Status = s
	js.CompletedAt = &now
}

type Task struct {
	JobID     string `json:"jobId"`
	StageName string `json:"stageId"`
	ID        string `json:"id"`

	NodeID   string `json:"nodeId"`
	NodeHost string `json:"nodeHost"`

	SubmittedAt time.Time `json:"submittedAt"`
}

func NewTask(node *Node, job *Job, stage *Stage) *Task {
	return &Task{
		ID:          mustGenerateID("T"),
		StageName:   stage.Name,
		JobID:       job.ID,
		NodeID:      node.ID,
		NodeHost:    node.Host,
		SubmittedAt: time.Now(),
	}
}

func (t Task) Reference() TaskReference {
	return TaskReference{
		JobID:     t.JobID,
		StageName: t.StageName,
		TaskID:    t.ID,
	}
}

type TaskReference struct {
	JobID     string
	StageName string
	TaskID    string
}

func (tr TaskReference) String() string {
	return fmt.Sprintf("%s/%s/%s", tr.JobID, tr.StageName, tr.TaskID)
}

type TaskStatus struct {
	Status Status `json:"status"`
	Error  string `json:"error,omitempty"`

	CurrentProgress uint64 `json:"currentProgress"`
	TotalProgress   uint64 `json:"totalProgress"`

	Metrics Metrics `json:"metrics"`

	SubmittedAt *time.Time `json:"submittedAt"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

func (ts *TaskStatus) Complete(s Status) {
	now := time.Now()
	ts.Status = s
	ts.CompletedAt = &now
}
