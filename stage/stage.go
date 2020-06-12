package stage

import "github.com/therne/lrmr/transformation"

type Stage struct {
	Name string
	Step int

	// Dependencies are list of stages need to be executed before this stage.
	Dependencies []*Stage

	// Transformer is a function the stage executes.
	Transformation transformation.Transformation
}

// New creates a new stage.
func New(name string, tf transformation.Transformation, deps ...*Stage) *Stage {
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
