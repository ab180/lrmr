package job

import (
	"github.com/therne/lrmr/partitions"
	"github.com/therne/lrmr/transformer"
)

type StageID string

type Stage struct {
	Name string `json:"name"`
	Step int    `json:"step"`

	Inputs  []string `json:"inputs"`
	Outputs []string `json:"outputs"`

	// Transformer is a function the stage executes.
	Transformer transformer.Transformer

	// Partitions is a scheduled partition information.
	Partitions partitions.Partitions `json:"partitions"`
}

// NewStage creates new stage.
func NewStage(name string, step int, in, out []string, tf transformer.Transformer) *Stage {
	return &Stage{
		Name:        name,
		Step:        step,
		Inputs:      in,
		Outputs:     out,
		Transformer: tf,
	}
}

type StageStatus struct {
	baseStatus
	Errors []string `json:"errors,omitempty"`
}

func newStageStatus() *StageStatus {
	return &StageStatus{baseStatus: newBaseStatus()}
}
