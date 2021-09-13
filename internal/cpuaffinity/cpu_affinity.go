// Scheduler only supports linux systems.
// +build !linux

package cpuaffinity

import (
	"sync"
)

var log = logger.New("cpuaffinity")

// Scheduler is no-op on non-linux systems.
type Scheduler struct{}

var _once sync.Once

func NewScheduler() Scheduler {
	return Scheduler{}
}

func (s *Scheduler) Occupy(string) interface{} {
	_once.Do(func() {
		log.Warn("CPU affinity scheduling is disabled on non-linux systems")
	})
	return nil
}

func (s *Scheduler) Release(interface{}) {}
