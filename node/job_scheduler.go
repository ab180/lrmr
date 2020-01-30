package node

import (
	"context"
	"errors"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/thoas/go-funk"
	"math"
)

var (
	// ErrWorkloadRefNotFound is raised when workload reference is not found with given ID.
	ErrWorkloadRefNotFound = errors.New("workload reference not found")

	// ErrNoNodes is raised when there's no nodes running in given type.
	ErrNoNodes = errors.New("no nodes available")
)

type Job struct {
	Key  string
	Task string

	Complexity uint64

	AffinityRule map[string]string
}

type ScheduleResult struct {
	JobID          string
	ScheduledNodes []string
	Assignments    []*JobAssignment
}

type JobAssignment struct {
	WorkloadRef WorkloadRef
	Job         *Job
	Node        *Node
}

// JobScheduler schedules jobs to nodes in the cluster.
type JobScheduler interface {
	// ScheduleWorkloads assigns given workloads to given candidate nodes
	// by balancing them with each work's complexity.
	// It will return a list of workload reference ID, which is used for releasing the workload.
	ScheduleJobs(ctx context.Context, jobs []*Job) (ScheduleResult, error)

	// ReleaseWorkload releases a node's current workload by workload reference ID.
	ReleaseWorkload(ctx context.Context, ref WorkloadRef) error
}

// calculateResourceUtilization calculates given node's current workload (sum of assigned job's complexity).
func (m *manager) calculateResourceUtilization(ctx context.Context, nodeID string) (uint64, error) {
	key := fmt.Sprintf(workloadRefKeyPrefixTmpl, nodeID)
	items, err := m.coordinator.Scan(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("job status scan failed: %v", err)
	}

	resourceUtilization := uint64(0)
	for _, item := range items {
		var job Job
		if err := item.Unmarshal(&job); err != nil {
			return 0, fmt.Errorf("unmarshaling job %s: %w", string(item), err)
		}
		resourceUtilization += job.Complexity
	}
	return resourceUtilization, nil
}

func (m *manager) ScheduleJobs(ctx context.Context, jobs []*Job) (result ScheduleResult, err error) {
	allNodes, err := m.List(ctx)
	if err != nil {
		err = fmt.Errorf("failed to list nodes information: %v", err)
		return
	}
	if len(allNodes) == 0 {
		err = ErrNoNodes
		return
	}
	// add TTL for to workload references in etcd for fault tolerance
	// so the workload will be eventually released even if the node fails to release by itself.
	//ttl := int64(m.opt.WorkloadTimeToLive.Seconds())
	//lease, err := m.jobs.Grant(ctx, ttl)
	//if err != nil {
	//	return nil, fmt.Errorf("etcd lease failed: %v", err)
	//}
	result.JobID = mustGenerateID()
	workloadBatchWrites := make(map[string]interface{})
	selectedNodeCounter := make(map[string]bool)

	for _, job := range jobs {
		// lookup nodes by affinity rule
		var candidates []*Node
		for _, n := range allNodes {
			for tagKey, tagCondition := range job.AffinityRule {
				if n.Tag[tagKey] == tagCondition {
					candidates = append(candidates, n)
				}
			}
		}
		if len(candidates) == 0 {
			candidates = allNodes
		}

		freestNode := &Node{}
		minUtilization := uint64(math.MaxUint64)
		for _, n := range candidates {
			if minUtilization > n.ResourceUtilization {
				freestNode = n
				minUtilization = n.ResourceUtilization
			}
		}
		// TODO: dangerous mutation of node info
		freestNode.ResourceUtilization += job.Complexity

		ref := WorkloadRef{
			JobID:  result.JobID,
			NodeID: freestNode.ID,
		}
		workloadBatchWrites[ref.EtcdKey()] = job
		selectedNodeCounter[freestNode.ID] = true

		result.Assignments = append(result.Assignments, &JobAssignment{
			WorkloadRef: ref,
			Job:         job,
			Node:        freestNode,
		})

		m.log.Debug("Scheduled", logger.Attrs{
			"nodeID":            freestNode.ID,
			"workloadReference": ref,
		})
	}
	result.ScheduledNodes = funk.Keys(selectedNodeCounter).([]string)

	// batch update workloads
	if err = m.coordinator.BatchPut(ctx, workloadBatchWrites); err != nil {
		err = fmt.Errorf("workload update failed: %w", err)
	}
	return
}

func (m *manager) ReleaseWorkload(ctx context.Context, ref WorkloadRef) error {
	deleted, err := m.coordinator.Delete(ctx, ref.EtcdKey())
	if err != nil {
		return fmt.Errorf("etcd write failed: %v", err)
	} else if deleted == 0 {
		return ErrWorkloadRefNotFound
	}
	return nil
}
