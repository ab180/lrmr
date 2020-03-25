package job

import (
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"strconv"
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
	Partition  lrmrpb.Output_PartitionerType `json:"partition"`
	FiniteKeys []string                      `json:"finiteKeys,omitempty"`

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
	so.Partition = lrmrpb.Output_FINITE_KEY
	so.FiniteKeys = keys
	return so
}

func (so *StageOutput) WithPartitions() *StageOutput {
	so.Partition = lrmrpb.Output_HASH_KEY
	return so
}

func (so *StageOutput) NoPartition() *StageOutput {
	return so
}

func (so *StageOutput) WithCollector() *StageOutput {
	so.Collect = true
	return so
}

func (so *StageOutput) Build(stageName string, workers []*node.Node) *lrmrpb.Output {
	// TODO: scheduling
	out := &lrmrpb.Output{
		Type:            lrmrpb.Output_PUSH,
		StageName:       stageName,
		Partitioner:     so.Partition,
		PartitionToHost: make(map[string]string),
	}
	if so.NoOutput {
		return out
	}
	if so.Partition == lrmrpb.Output_FINITE_KEY {
		for i, key := range so.FiniteKeys {
			slot := i % len(workers)
			out.PartitionToHost[key] = workers[slot].Host
		}
	} else {
		// create partition with total number of executors
		i := 0
		for _, w := range workers {
			for n := 0; n < w.Executors; n++ {
				key := strconv.Itoa(i)
				out.PartitionToHost[key] = w.Host
				i += 1
			}
		}
	}
	return out
}
