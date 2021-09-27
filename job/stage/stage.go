package stage

import (
	"github.com/ab180/lrmr/internal/serialization"
	"github.com/ab180/lrmr/partitions"
	"github.com/ab180/lrmr/transformation"
)

type Stage struct {
	Name string `json:"name"`

	// Input are list of stages need to be executed before this stage.
	Inputs []*Input `json:"inputs"`

	// Function is a transformation the stage executes.
	Function transformation.Serializable `json:"function"`

	Output Output
}

// New creates a new stage.
func New(name string, fn transformation.Transformation, in ...*Input) *Stage {
	return &Stage{
		Name:     name,
		Inputs:   in,
		Function: transformation.Serializable{Transformation: fn},
	}
}

func (s *Stage) SetOutputTo(dest *Stage) {
	s.Output.Stage = dest.Name
	// s.Output.Type
}

type Input struct {
	Stage string             `json:"stage"`
	Type  serialization.Type `json:"type"`
}

func InputFrom(s *Stage) *Input {
	return &Input{
		Stage: s.Name,
		// Type:  s.Output.Type,
	}
}

type Output struct {
	Stage string             `json:"stage"`
	Type  serialization.Type `json:"type"`

	Partitioner partitions.SerializablePartitioner `json:"partitioner"`
}
