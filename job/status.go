package job

import (
	"fmt"
	"io"
	"time"

	"go.uber.org/atomic"
)

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

// Status is a status of the job.
type Status struct {
	baseStatus
	Errors []Error `json:"errors,omitempty"`
}

func newStatus() *Status {
	return &Status{baseStatus: newBaseStatus()}
}

type StageStatus struct {
	baseStatus
	DoneTasks atomic.Int32 `json:"doneTasks"`
}

func newStageStatus() *StageStatus {
	return &StageStatus{baseStatus: newBaseStatus()}
}

// Error is an error caused job to stop.
type Error struct {
	Task       string
	Message    string
	Stacktrace string
}

func (e Error) Error() string {
	return fmt.Sprintf("%s (%s)", e.Task, e.Message)
}

func (e Error) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			_, _ = io.WriteString(s, fmt.Sprintf("(from %s) %s", e.Message, e.Stacktrace))
			return
		}
		fallthrough
	case 's', 'q':
		_, _ = io.WriteString(s, e.Error())
	}
}
