package job

import (
	"fmt"
	"time"

	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/stage"
)

type Task struct {
	JobID       string    `json:"jobId"`
	StageName   string    `json:"stageId"`
	PartitionID string    `json:"id"`
	NodeHost    string    `json:"nodeHost"`
	SubmittedAt time.Time `json:"submittedAt"`
}

func NewTask(partitionKey string, node *node.Node, jobID string, stage *stage.Stage) *Task {
	return &Task{
		PartitionID: partitionKey,
		StageName:   stage.Name,
		JobID:       jobID,
		NodeHost:    node.Host,
		SubmittedAt: time.Now(),
	}
}

func (t Task) ID() TaskID {
	return TaskID{
		JobID:       t.JobID,
		StageName:   t.StageName,
		PartitionID: t.PartitionID,
	}
}

type TaskID struct {
	JobID       string
	StageName   string
	PartitionID string
}

func (tid TaskID) String() string {
	return fmt.Sprintf("%s/%s/%s", tid.JobID, tid.StageName, tid.PartitionID)
}

func (tid TaskID) WithoutJobID() string {
	return fmt.Sprintf("%s/%s", tid.StageName, tid.PartitionID)
}

type TaskStatus struct {
	baseStatus
	Error   string  `json:"error,omitempty"`
	Metrics Metrics `json:"metrics"`
}

func NewTaskStatus() *TaskStatus {
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
