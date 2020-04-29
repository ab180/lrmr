package master

import (
	"context"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/job/partitions"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
)

var ErrNoAvailableWorkers = errors.New("no available workers")

var log = logger.New("master")

type Master struct {
	collector *Collector

	JobManager   job.Manager
	JobTracker   *job.Tracker
	JobReporter  *job.Reporter
	JobScheduler *job.Assigner
	NodeManager  node.Manager

	opt Options
}

func New(crd coordinator.Coordinator, opt Options) (*Master, error) {
	nm, err := node.NewManager(crd, opt.RPC)
	if err != nil {
		return nil, err
	}
	m := &Master{
		JobManager:   job.NewManager(nm, crd),
		JobTracker:   job.NewJobTracker(crd),
		JobReporter:  job.NewJobReporter(crd),
		JobScheduler: job.NewAssigner(nm),
		NodeManager:  nm,
		opt:          opt,
	}
	m.collector = NewCollector(m)
	return m, nil
}

func (m *Master) Start() {
	if err := m.collector.Start(); err != nil {
		log.Error("Failed to start collector RPC server", err)
	}
	go m.JobTracker.HandleJobCompletion()
}

func (m *Master) CreateJob(ctx context.Context, name string, plans []job.PartitionPlan, stages []*job.Stage) ([]partitions.PhysicalPlans, *job.Job, error) {
	workers, err := m.NodeManager.List(ctx, node.Worker)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "list available workers")
	}
	if len(workers) == 0 {
		return nil, nil, ErrNoAvailableWorkers
	}
	sched, err := partitions.NewSchedulerWithNodes(ctx, nil, workers)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "start scheduling")
	}

	physicalPlans := make([]partitions.PhysicalPlans, len(plans))
	for i, p := range plans {
		if i == len(stages) {
			// collect stage
			p.PlanOptions = []partitions.PlanOption{
				partitions.WithFixedNodeGroups(m.collector.Node),
				partitions.WithFixedCount(1),
				partitions.WithExecutorsPerNode(1),
			}
		}
		if p.Partitioner == lrmrpb.Output_PRESERVE {
			physicalPlans[i] = physicalPlans[i-1]
			if i > 1 {
				stages[i-1].Partitions = stages[i-2].Partitions
			}
		} else {
			logical, physical := sched.Plan(p.PlanOptions...)
			physicalPlans[i] = physical
			if i > 0 {
				stages[i-1].Partitions = logical
			}
		}
		var stageName string
		if i < len(stages) {
			stageName = stages[i].Name
		} else {
			stageName = CollectStageName
		}
		log.Verbose("Planned {} partitions on {}/{} (input by {}):\n{}", len(physicalPlans[i]), name, stageName, p.Partitioner, physicalPlans[i].Pretty())
	}

	j, err := m.JobManager.CreateJob(ctx, name, stages)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "create job")
	}
	m.JobTracker.AddJob(j)
	m.collector.Collect(j.ID)

	return physicalPlans, j, nil
}

func (m *Master) CollectedResults(jobID string) (<-chan map[string][]*lrdd.Row, error) {
	return m.collector.Results(jobID)
}

func (m *Master) Stop() {
	m.JobTracker.Close()
	if err := m.NodeManager.Close(); err != nil {
		log.Error("failed to close node manager", err)
	}
}
