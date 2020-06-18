package job

import (
	"time"

	"github.com/therne/lrmr/stage"
)

type Job struct {
	ID          string        `json:"id"`
	Name        string        `json:"name"`
	Stages      []stage.Stage `json:"stages"`
	SubmittedAt time.Time     `json:"submittedAt"`
}

func (j *Job) GetStage(name string) *stage.Stage {
	for _, s := range j.Stages {
		if s.Name == name {
			return &s
		}
	}
	return nil
}
