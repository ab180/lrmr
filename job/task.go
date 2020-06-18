package job

import (
	"fmt"
	"net/url"
	"time"

	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/stage"
)

type Task struct {
	JobID       string `json:"jobId"`
	StageName   string `json:"stageId"`
	PartitionID string `json:"id"`

	NodeID   string `json:"nodeId"`
	NodeHost string `json:"nodeHost"`

	SubmittedAt time.Time `json:"submittedAt"`
}

func NewTask(partitionKey string, node *node.Node, jobID string, stage stage.Stage) *Task {
	return &Task{
		PartitionID: partitionKey,
		StageName:   stage.Name,
		JobID:       jobID,
		NodeID:      node.ID,
		NodeHost:    node.Host,
		SubmittedAt: time.Now(),
	}
}

func (t *Task) ID() string {
	return url.QueryEscape(t.PartitionID)
}

func (t Task) Reference() TaskReference {
	return TaskReference{
		JobID:     t.JobID,
		StageName: t.StageName,
		TaskID:    t.ID(),
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
	baseStatus
	Error   string  `json:"error,omitempty"`
	Metrics Metrics `json:"metrics"`
}

func newTaskStatus() *TaskStatus {
	return &TaskStatus{
		baseStatus: newBaseStatus(),
		Metrics:    make(Metrics),
	}
}

func (ts TaskStatus) Clone() TaskStatus {
	m := make(Metrics)
	for k, v := range ts.Metrics {
		m[k] = v
	}
	return TaskStatus{
		baseStatus: ts.baseStatus,
		Error:      ts.Error,
		Metrics:    m,
	}
}
