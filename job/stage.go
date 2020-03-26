package job

import (
	"github.com/therne/lrmr/job/partitions"
	"time"
)

type Stage struct {
	Name       string                  `json:"name"`
	RunnerName string                  `json:"runnerName"`
	Partitions partitions.LogicalPlans `json:"partitions"`
	StartedAt  *time.Time              `json:"startedAt,omitempty"`
}

// NewStage creates new stage.
// JobIDs and workers will be filled by JobManager.
func NewStage(name, runnerName string) *Stage {
	return &Stage{
		Name:       name,
		RunnerName: runnerName,
	}
}

type StageStatus struct {
	baseStatus
	Errors []string `json:"errors,omitempty"`
}

func newStageStatus() *StageStatus {
	return &StageStatus{baseStatus: newBaseStatus()}
}
