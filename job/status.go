package job

import "time"

type RunningState string

const (
	Starting  RunningState = "starting"
	Running   RunningState = "running"
	Failed    RunningState = "failed"
	Succeeded RunningState = "succeeded"
)

type baseStatus struct {
	Status      RunningState `json:"status"`
	SubmittedAt time.Time    `json:"submittedAt"`
	CompletedAt *time.Time   `json:"completedAt,omitempty"`
}

func newBaseStatus() baseStatus {
	return baseStatus{
		Status:      Starting,
		SubmittedAt: time.Now(),
	}
}

func (s *baseStatus) Complete(rs RunningState) {
	now := time.Now()
	s.Status = rs
	s.CompletedAt = &now
}
