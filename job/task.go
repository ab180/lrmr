package job

import (
	"fmt"
	"time"

	"github.com/ab180/lrmr/cluster/node"
	stage2 "github.com/ab180/lrmr/job/stage"
)

type Task struct {
	JobID       string    `json:"jobId"`
	StageName   string    `json:"stageId"`
	PartitionID string    `json:"id"`
	NodeHost    string    `json:"nodeHost"`
	SubmittedAt time.Time `json:"submittedAt"`
}

func NewTask(partitionKey string, node *node.Node, jobID string, stage *stage2.Stage) *Task {
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
