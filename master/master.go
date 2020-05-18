package master

import (
	"context"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/partitions"
	"golang.org/x/sync/errgroup"
	"path"
	"sync"
)

var ErrNoAvailableWorkers = errors.New("no available workers")

var log = logger.New("master")

type Master struct {
	collector *Collector

	JobManager  *job.Manager
	JobTracker  *job.Tracker
	JobReporter *job.Reporter
	NodeManager node.Manager

	opt Options
}

func New(crd coordinator.Coordinator, opt Options) (*Master, error) {
	nm, err := node.NewManager(crd, opt.RPC)
	if err != nil {
		return nil, err
	}
	m := &Master{
		JobManager:  job.NewManager(nm, crd),
		JobTracker:  job.NewJobTracker(crd),
		JobReporter: job.NewJobReporter(crd),
		NodeManager: nm,
		opt:         opt,
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

func (m *Master) CreateJob(ctx context.Context, name string, plans []partitions.Plan, stages []*job.Stage) ([]partitions.Assignments, *job.Job, error) {
	workers, err := m.NodeManager.List(ctx, node.Worker)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "list available workers")
	}
	if len(workers) == 0 {
		return nil, nil, ErrNoAvailableWorkers
	}

	// collect stage
	plans = append(plans, partitions.Plan{
		DesiredCount:     1,
		MaxNodes:         1,
		ExecutorsPerNode: 1,
	})
	// TODO: desired node: partitions.WithFixedNodeGroups(m.collector.Node),

	pp, assignments := partitions.Schedule(workers, plans)
	for i, p := range pp {
		var stageName string
		if i < len(stages) {
			stages[i].Partitions = p
			stageName = stages[i].Name
		} else {
			stageName = CollectStageName
		}
		log.Verbose("Planned {} partitions on {}/{} (input by {}):\n{}", len(p.Partitions), name, stageName, p.Partitioner, assignments[i].Pretty())
	}

	j, err := m.JobManager.CreateJob(ctx, name, stages)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "create job")
	}
	m.JobTracker.AddJob(j)
	m.collector.Collect(j.ID)

	return assignments, j, nil
}

// StartTasks create tasks to the nodes with the plan.
func (m *Master) StartJob(ctx context.Context, j *job.Job, assignments []partitions.Assignments, broadcasts map[string][]byte) error {
	// initialize tasks reversely, so that outputs can be connected with next stage
	for i := len(j.Stages) - 1; i >= 0; i-- {
		wg, wctx := errgroup.WithContext(ctx)
		for _, curPartition := range assignments[i] {
			w := curPartition.Node
			req := &lrmrpb.CreateTaskRequest{
				JobID:       j.ID,
				StageName:   j.Stages[i].Name,
				PartitionID: curPartition.ID,
				Input:       buildInputAt(j.Stages, i),
				Output:      buildOutputTo(j.Stages, i+1, assignments[i+1]),
				Broadcasts:  broadcasts,
			}
			wg.Go(func() error {
				conn, err := m.NodeManager.Connect(wctx, w.Host)
				if err != nil {
					return errors.Wrapf(err, "dial %s for stage %s", w.Host, req.StageName)
				}
				if _, err := lrmrpb.NewNodeClient(conn).CreateTask(wctx, req); err != nil {
					return errors.Wrapf(err, "call CreateTask on %s", w.Host)
				}
				return nil
			})
		}
		if err := wg.Wait(); err != nil {
			return err
		}
	}
	return nil
}

func buildInputAt(stages []*job.Stage, stageIdx int) []*lrmrpb.Input {
	var prevStageName string
	if stageIdx == 0 {
		prevStageName = "__input"
	} else {
		prevStageName = stages[stageIdx-1].Name
	}
	return []*lrmrpb.Input{
		{Type: lrmrpb.Input_PUSH, PrevStageName: prevStageName},
	}
}

func buildOutputTo(stages []*job.Stage, nextStageIdx int, nextPartitions partitions.Assignments) []*lrmrpb.Output {
	var nextStageName string
	if nextStageIdx < len(stages) {
		nextStageName = stages[nextStageIdx].Name
	} else {
		nextStageName = "__collect"
	}
	return []*lrmrpb.Output{
		{
			Type:            lrmrpb.Output_PUSH,
			PartitionToHost: nextPartitions.ToMap(),
			NextStageName:   nextStageName,
		},
	}
}

func (m *Master) OpenInputWriter(ctx context.Context, j *job.Job, targets partitions.Assignments, partitioner partitions.Partitioner) (output.Output, error) {
	outs := make(map[string]output.Output)
	var lock sync.Mutex

	wg, reqCtx := errgroup.WithContext(ctx)
	for _, t := range targets {
		p := t
		wg.Go(func() error {
			taskID := path.Join(j.ID, j.Stages[0].Name, p.ID)
			out, err := output.NewPushStream(reqCtx, m.NodeManager, p.Node.Host, taskID)
			if err != nil {
				return errors.Wrapf(err, "connect %s", p.Node.Host)
			}
			lock.Lock()
			outs[p.ID] = out
			lock.Unlock()
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	out := output.NewWriter(nil /* TODO */, partitioner, outs)
	return out, nil
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
