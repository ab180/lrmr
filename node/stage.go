package node

import (
	"github.com/therne/lrmr/lrmrpb"
	"time"
)

type Stage struct {
	Name       string       `json:"name"`
	RunnerName string       `json:"runnerName"`
	Output     *StageOutput `json:"output"`
	Workers    []*Node      `json:"workers"`
	StartedAt  *time.Time   `json:"startedAt,omitempty"`
}

// NewStage creates new stage.
// JobIDs and workers will be filled by JobManager.
func NewStage(name, runnerName string, output *StageOutput) *Stage {
	return &Stage{
		Name:       name,
		RunnerName: runnerName,
		Output:     output,
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

type StageOutput struct {
	PartitionerType lrmrpb.Partitioner_Type `json:"partitioner"`
	KeyColumn       string                  `json:"keyColumn,omitempty"`
	FiniteKeys      []string                `json:"finiteKeys,omitempty"`

	// noOutput makes output shard empty on Build.
	NoOutput bool `json:"-"`
	Collect  bool `json:"-"`
}

func DescribingStageOutput() *StageOutput {
	return &StageOutput{}
}

func (so *StageOutput) Nothing() *StageOutput {
	so.NoOutput = true
	return so
}

func (so *StageOutput) WithFixedPartitions(keyColumn string, keys []string) *StageOutput {
	so.PartitionerType = lrmrpb.Partitioner_FINITE_KEY
	so.KeyColumn = keyColumn
	so.FiniteKeys = keys
	return so
}

func (so *StageOutput) WithPartitions(keyColumn string) *StageOutput {
	so.PartitionerType = lrmrpb.Partitioner_HASH_KEY
	so.KeyColumn = keyColumn
	return so
}

func (so *StageOutput) WithFanout() *StageOutput {
	return so
}

func (so *StageOutput) WithCollector() *StageOutput {
	so.Collect = true
	return so
}

func (so *StageOutput) Build(shards []*lrmrpb.HostMapping) *lrmrpb.Output {
	out := &lrmrpb.Output{
		Shards: shards,
		Partitioner: &lrmrpb.Partitioner{
			Type: so.PartitionerType,
		},
	}
	if so.PartitionerType != lrmrpb.Partitioner_NONE {
		out.Partitioner.KeyColumn = so.KeyColumn
		out.Partitioner.KeyToHost = make(map[string]string)
		for i, key := range so.FiniteKeys {
			out.Partitioner.KeyToHost[key] = shards[i%len(shards)].Host
		}
	}
	return out
}
