package job

import (
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"time"
)

type Stage struct {
	Name       string       `json:"name"`
	RunnerName string       `json:"runnerName"`
	Output     *StageOutput `json:"output"`
	Workers    []*node.Node `json:"workers"`
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
	baseStatus
	Errors []string `json:"errors,omitempty"`
}

func newStageStatus() *StageStatus {
	return &StageStatus{baseStatus: newBaseStatus()}
}

type StageOutput struct {
	Partition  lrmrpb.Partitioner_Type `json:"partition"`
	FiniteKeys []string                `json:"finiteKeys,omitempty"`

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

func (so *StageOutput) WithFixedPartitions(keys []string) *StageOutput {
	so.Partition = lrmrpb.Partitioner_FINITE_KEY
	so.FiniteKeys = keys
	return so
}

func (so *StageOutput) WithPartitions() *StageOutput {
	so.Partition = lrmrpb.Partitioner_HASH_KEY
	return so
}

func (so *StageOutput) NoPartition() *StageOutput {
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
			Type: so.Partition,
		},
	}
	if so.Partition != lrmrpb.Partitioner_NONE {
		out.Partitioner.KeyToHost = make(map[string]string)
		for i, key := range so.FiniteKeys {
			out.Partitioner.KeyToHost[key] = shards[i%len(shards)].Host
		}
	}
	return out
}
