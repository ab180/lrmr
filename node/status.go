package node

import "time"

type Status string

const (
	Starting  Status = "starting"
	Running   Status = "running"
	Failed    Status = "failed"
	Succeeded Status = "succeeded"
)

func now() *time.Time {
	n := time.Now()
	return &n
}
