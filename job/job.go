package job

import (
	"time"

	"github.com/ab180/lrmr/partitions"
	"github.com/ab180/lrmr/stage"
)

type Job struct {
	ID          string                   `json:"id"`
	Name        string                   `json:"name"`
	Stages      []stage.Stage            `json:"stages"`
	Partitions  []partitions.Assignments `json:"partitions"`
	SubmittedAt time.Time                `json:"submittedAt"`
}

func (j *Job) GetStage(name string) *stage.Stage {
	for _, s := range j.Stages {
		if s.Name == name {
			return &s
		}
	}
	return nil
}

func (j *Job) GetPartitionsOfStage(name string) partitions.Assignments {
	for i, s := range j.Stages {
		if s.Name == name {
			return j.Partitions[i]
		}
	}
	return nil
}
