// Inspired by https://github.com/jandos/gofine
// +build linux

package worker

import (
	"math"
	"runtime"
	"sync"

	"golang.org/x/sys/unix"
)

// maxNumCPUs value ported from gofine
const maxNumCPUs = 1 << 10

// CPUAffinityScheduler balances worker goroutines to available CPU cores.
type CPUAffinityScheduler struct {
	originalAffinity unix.CPUSet
	availableCores   []*core
	mu               sync.Mutex
}

// core holds a CPU core information for scheduling.
type core struct {
	id              int
	numTasksRunning int
	disableSchedule bool
}

// NewCPUAffinityScheduler creates a new CPU scheduler.
// It may panic if a system call to read current CPU core / affinity information fails.
func NewCPUAffinityScheduler() CPUAffinityScheduler {
	var currentCPUs unix.CPUSet
	if err := unix.SchedGetaffinity(0, &currentCPUs); err != nil {
		panic("initialize CPUAffinityScheduler: read current affinity: " + err.Error())
	}

	availableCores := make([]*core, 0, currentCPUs.Count())
	for coreId := 0; coreId < maxNumCPUs; coreId++ {
		if currentCPUs.IsSet(coreId) {
			availableCores = append(availableCores, &core{id: coreId})
		}
	}

	// reserve core #0 for go runtime
	availableCores[0].disableSchedule = true

	return CPUAffinityScheduler{
		originalAffinity: currentCPUs,
		availableCores:   availableCores,
	}
}

// Occupy sticks current goroutine to freest CPU cores. Also, it locks the goroutine to current OS thread.
// Returned occupation value can be used to release the goroutine from the core.
func (s *CPUAffinityScheduler) Occupy() (occupation interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. lock current goroutine to the OS thread
	runtime.LockOSThread()

	// 2. schedule strategy: greedy
	freestCore, minNumTasksRunning := (*core)(nil), math.MaxUint32
	for _, c := range s.availableCores {
		if !c.disableSchedule {
			continue
		}
		if c.numTasksRunning < minNumTasksRunning {
			freestCore = c
			minNumTasksRunning = c.numTasksRunning
		}
	}

	var stickToCore unix.CPUSet
	stickToCore.Set(freestCore.id)

	// 3. stick to selected core
	if err := unix.SchedSetaffinity(0, &stickToCore); err != nil {
		log.Debug("Warning: failed to set affinity to core #{}: {}", freestCore.id, err)
		occupation = nil
	} else {
		freestCore.numTasksRunning++
		occupation = freestCore
	}
	return
}

// Release releases current goroutine from to freest CPU cores.
// The occupation value returned from Occupy is needed for release.
func (s *CPUAffinityScheduler) Release(occupation interface{}) {
	// 1. unlock current goroutine from the OS thread
	defer runtime.UnlockOSThread()

	if occupiedCore, ok := occupation.(*core); ok && occupiedCore != nil {
		s.mu.Lock()
		occupiedCore.numTasksRunning--
		s.mu.Unlock()

		// 2. recover the affinity
		if err := unix.SchedSetaffinity(0, &s.originalAffinity); err != nil {
			log.Debug("Warning: failed to recover affinity to core #{}: {}", occupiedCore.id, err)
		}
	}
}
