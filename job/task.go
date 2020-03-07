package job

import (
	"fmt"
	"github.com/therne/lrmr/internal/utils"
	"github.com/therne/lrmr/node"
	"time"
)

type Task struct {
	JobID     string `json:"jobId"`
	StageName string `json:"stageId"`
	ID        string `json:"id"`

	NodeID   string `json:"nodeId"`
	NodeHost string `json:"nodeHost"`

	SubmittedAt time.Time `json:"submittedAt"`
}

func NewTask(node *node.Node, job *Job, stage *Stage) *Task {
	return &Task{
		ID:          utils.GenerateID("T"),
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
	baseStatus
	Error string `json:"error,omitempty"`

	CurrentProgress uint64  `json:"currentProgress"`
	TotalProgress   uint64  `json:"totalProgress"`
	Metrics         Metrics `json:"metrics"`
}

func newTaskStatus() *TaskStatus {
	return &TaskStatus{
		baseStatus: newBaseStatus(),
		Metrics:    make(Metrics),
	}
}
