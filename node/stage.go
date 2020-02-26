package node

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/therne/lrmr/lrmrpb"
	"time"
)

type Stage struct {
	Name           string       `json:"name"`
	Transformation string       `json:"transformation"`
	Output         *StageOutput `json:"output"`
	Workers        []*Node      `json:"workers"`
	StartedAt      *time.Time   `json:"startedAt,omitempty"`
}

// NewStage creates new stage.
// JobIDs and workers will be filled by JobManager.
func NewStage(name, transformationID string, output *StageOutput) *Stage {
	return &Stage{
		Name:           name,
		Transformation: transformationID,
		Output:         output,
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
	outputType lrmrpb.Output_Type

	partitionerType lrmrpb.Partitioner_Type
	partitionColumn string
	partitionKeys   []string
}

func DescribingStageOutput() *StageOutput {
	return &StageOutput{}
}

func (so *StageOutput) Nothing() *StageOutput {
	so.outputType = lrmrpb.Output_NONE
	return so
}

func (so *StageOutput) WithFixedPartitions(keyColumn string, keys []string) *StageOutput {
	so.outputType = lrmrpb.Output_PARTITIONER
	so.partitionerType = lrmrpb.Partitioner_FINITE_KEY
	so.partitionColumn = keyColumn
	so.partitionKeys = keys
	return so
}

func (so *StageOutput) WithPartitions(keyColumn string) *StageOutput {
	so.outputType = lrmrpb.Output_PARTITIONER
	so.partitionerType = lrmrpb.Partitioner_HASH
	so.partitionColumn = keyColumn
	return so
}

func (so *StageOutput) WithRoundRobin() *StageOutput {
	so.outputType = lrmrpb.Output_ROUND_ROBIN
	return so
}

func (so *StageOutput) WithCollector() *StageOutput {
	so.outputType = lrmrpb.Output_COLLECTOR
	return so
}

func (so *StageOutput) Build(master *Node, shards []*Node) *lrmrpb.Output {
	out := &lrmrpb.Output{
		Type: so.outputType,
	}
	switch out.Type {
	case lrmrpb.Output_PARTITIONER:
		out.Partitioner = &lrmrpb.Partitioner{
			Type:               so.partitionerType,
			PartitionKeyColumn: so.partitionColumn,
			PartitionKeyToHost: make(map[string]string),
		}
		for i, key := range so.partitionKeys {
			out.Partitioner.PartitionKeyToHost[key] = shards[i%len(shards)].Host
		}
	case lrmrpb.Output_COLLECTOR:
		out.Collector.DriverHost = master.Host
	}
	return out
}

func (so *StageOutput) MarshalJSON() ([]byte, error) {
	msg := map[string]interface{}{
		"type": so.outputType.String(),
	}
	if so.outputType == lrmrpb.Output_PARTITIONER {
		msg["partitioner"] = map[string]interface{}{
			"type":   so.partitionerType.String(),
			"column": so.partitionColumn,
			"keys":   so.partitionKeys,
		}
	}
	return jsoniter.Marshal(msg)
}

func (so *StageOutput) UnmarshalJSON(data []byte) error {
	var msg struct {
		Type        string `json:"type"`
		Partitioner struct {
			Type   string   `json:"type"`
			Column string   `json:"column"`
			Keys   []string `json:"keys"`
		} `json:"partitioner"`
	}
	if err := jsoniter.Unmarshal(data, &msg); err != nil {
		return err
	}
	so.outputType = lrmrpb.Output_Type(lrmrpb.Output_Type_value[msg.Type])
	if so.outputType == lrmrpb.Output_PARTITIONER {
		so.partitionerType = lrmrpb.Partitioner_Type(lrmrpb.Partitioner_Type_value[msg.Partitioner.Type])
		so.partitionColumn = msg.Partitioner.Column
		so.partitionKeys = msg.Partitioner.Keys
	}
	return nil
}
