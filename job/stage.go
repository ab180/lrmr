package job

import (
	"github.com/therne/lrmr/partitions"
	"github.com/therne/lrmr/transformation"
)

type Stage struct {
	Name string `json:"name"`
	Step int    `json:"step"`

	// Dependencies are list of stages need to be executed before this stage.
	Dependencies []*Stage `json:"-"`

	// Transformer is a function the stage executes.
	Transformation transformation.Transformation

	// Partitions is a scheduled partition informations.
	Partitions partitions.Partitions `json:"partitions"`
}

// NewStage creates new stage.
func NewStage(name string, tf transformation.Transformation, deps ...*Stage) *Stage {
	maxStep := -1
	for _, dep := range deps {
		if maxStep < dep.Step {
			maxStep = dep.Step
		}
	}
	return &Stage{
		Name:           name,
		Step:           maxStep + 1,
		Dependencies:   deps,
		Transformation: tf,
	}
}

type StageStatus struct {
	baseStatus
	Errors []string `json:"errors,omitempty"`
}

func newStageStatus() *StageStatus {
	return &StageStatus{baseStatus: newBaseStatus()}
}
