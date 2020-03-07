package job

import (
	"github.com/therne/lrmr/node"
	"time"
)

type Job struct {
	ID   string `json:"id"`
	Name string `json:"name"`

	Stages  []*Stage     `json:"stages"`
	Workers []*node.Node `json:"workers"`

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

// Status is a status of the job.
type Status struct {
	baseStatus
}

func newStatus() *Status {
	return &Status{newBaseStatus()}
}
