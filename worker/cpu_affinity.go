// CPUAffinityScheduler only supports linux systems.
// +build !linux

package worker

import (
	"sync"
)

// CPUAffinityScheduler is no-op on non-linux systems.
type CPUAffinityScheduler struct{}

var _once sync.Once

func NewCPUAffinityScheduler() CPUAffinityScheduler {
	return CPUAffinityScheduler{}
}

func (s *CPUAffinityScheduler) Occupy(string) interface{} {
	_once.Do(func() {
		log.Warn("CPU affinity scheduling is disabled on non-linux systems")
	})
	return nil
}

func (s *CPUAffinityScheduler) Release(interface{}) {}
