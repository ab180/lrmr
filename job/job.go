package job

import (
	"time"

	"github.com/ab180/lrmr/job/stage"
	"github.com/ab180/lrmr/lrmrpb"
	"github.com/ab180/lrmr/partitions"
	"github.com/samber/lo"
)

type Job struct {
	ID          string                   `json:"id"`
	Stages      []*stage.Stage           `json:"stages"`
	Partitions  []partitions.Assignments `json:"partitions"`
	SubmittedAt time.Time                `json:"submittedAt"`
}

func (j *Job) GetStage(name string) *stage.Stage {
	for _, s := range j.Stages {
		if s.Name == name {
			return s
		}
	}
	return nil
}

func (j *Job) GetPrevStageOf(name string) *stage.Stage {
	for i, s := range j.Stages {
		if s.Name == name && i > 0 {
			return j.Stages[i-1]
		}
	}
	return nil
}

func (j *Job) GetPartitionsOfStage(name string) partitions.Assignments {
	for i, s := range j.Stages {
		if s.Name == name {
			return j.Partitions[i]
		}
	}
	return nil
}

func (j *Job) Hostnames() []string {
	var hostnames []string
	for _, pp := range j.Partitions {
		for _, hostname := range pp.Hostnames() {
			if !lo.Contains(hostnames, hostname) {
				hostnames = append(hostnames, hostname)
			}
		}
	}
	return hostnames
}

// BuildStageDefinitionPerNode returns lrmrpb.Stage protobuf definitions per executor node.
func (j *Job) BuildStageDefinitionPerNode() map[string][]*lrmrpb.Stage {
	stagesByNode := make(map[string][]*lrmrpb.Stage)
	for i, stageDesc := range j.Stages {
		if i == 0 {
			continue
		}
		for host, partitionIDs := range j.Partitions[i].GroupIDsByHost() {
			tasks := make([]*lrmrpb.Task, len(partitionIDs))
			for i, partitionID := range partitionIDs {
				tasks[i] = &lrmrpb.Task{
					PartitionID: partitionID,
				}
			}
			stageDef := &lrmrpb.Stage{
				Name:  stageDesc.Name,
				Tasks: tasks,
				Input: []*lrmrpb.Input{
					{Type: lrmrpb.Input_PUSH},
				},
				Output: &lrmrpb.Output{
					Type: lrmrpb.Output_PUSH,
				},
			}
			if i < len(j.Stages)-1 {
				stageDef.Output.PartitionToHost = j.Partitions[i+1].ToMap()
			} else {
				stageDef.Output.PartitionToHost = make(map[string]string, 0)
			}
			stagesByNode[host] = append(stagesByNode[host], stageDef)
		}
	}
	return stagesByNode
}
