package node

import (
	"time"
)

type Stage struct {
	Name           string     `json:"name"`
	Transformation string     `json:"transformation"`
	Workers        []*Node    `json:"workers"`
	StartedAt      *time.Time `json:"startedAt,omitempty"`
}

// NewStage creates new stage.
// JobIDs and workers will be filled by JobManager.
func NewStage(name, transformationID string) *Stage {
	return &Stage{
		Name:           name,
		Transformation: transformationID,
	}
}

type StageStatus struct {
	Status Status   `json:"status"`
	Errors []string `json:"errors,omitempty"`

	SubmittedAt *time.Time `json:"submittedAt"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

func (ss *StageStatus) Complete(s Status) {
	now := time.Now()
	ss.Status = s
	ss.CompletedAt = &now
}
