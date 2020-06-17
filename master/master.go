package master

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/partitions"
	"github.com/therne/lrmr/transformation"
	"golang.org/x/sync/errgroup"
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

	// plans collect stage to the master
	plans[len(plans)-1].Partitioner = NewCollectPartitioner()
	plans = append(plans, partitions.Plan{
		DesiredCount:     1,
		MaxNodes:         1,
		ExecutorsPerNode: 1,
	})
	stages = append(stages, job.NewStage(CollectStageName, nil))

	pp, assignments := partitions.Schedule(workers, plans, partitions.WithMaster(m.collector.Node))
	for i, p := range pp {
		stages[i].Partitions = p

		partitionerName := fmt.Sprintf("%T", partitions.UnwrapPartitioner(p.Partitioner))
		log.Verbose("Planned {} partitions on {}/{} (output by {}):\n{}", len(p.Partitions),
			name, stages[i].Name, partitionerName, assignments[i].Pretty())
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
	for i := len(j.Stages) - 1; i >= 1; i-- {
		wg, wctx := errgroup.WithContext(ctx)
		for _, curPartition := range assignments[i] {
			w := curPartition.Node
			req := &lrmrpb.CreateTaskRequest{
				JobID:       j.ID,
				StageName:   j.Stages[i].Name,
				PartitionID: curPartition.ID,
				Input:       []*lrmrpb.Input{{Type: lrmrpb.Input_PUSH, PrevStageName: j.Stages[i-1].Name}},
				Output:      buildOutputTo(j.Stages, assignments, i+1),
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

func buildOutputTo(stages []*job.Stage, assignments []partitions.Assignments, nextStageIdx int) []*lrmrpb.Output {
	if nextStageIdx == len(stages) {
		return []*lrmrpb.Output{{Type: lrmrpb.Output_PUSH}}
	}
	return []*lrmrpb.Output{
		{
			Type:            lrmrpb.Output_PUSH,
			PartitionToHost: assignments[nextStageIdx].ToMap(),
			NextStageName:   stages[nextStageIdx].Name,
		},
	}
}

func (m *Master) OpenInputWriter(ctx context.Context, j *job.Job, stageName string, targets partitions.Assignments, partitioner partitions.Partitioner) (output.Output, error) {
	s := j.GetStage(stageName)
	if s == nil {
		return nil, errors.Errorf("stage %s not found", stageName)
	}

	outs := make(map[string]output.Output)
	var lock sync.Mutex

	wg, reqCtx := errgroup.WithContext(ctx)
	for _, t := range targets {
		p := t
		wg.Go(func() error {
			taskID := path.Join(j.ID, s.Name, p.ID)
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
	out := output.NewWriter(newContextForInput(ctx, len(s.Partitions.Partitions)), partitioner, outs)
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

type ctxForInput struct {
	context.Context
	numOutputs int
}

func newContextForInput(ctx context.Context, numOutputs int) transformation.Context {
	return &ctxForInput{
		Context:    ctx,
		numOutputs: numOutputs,
	}
}

func (i ctxForInput) Broadcast(key string) interface{}         { return nil }
func (i ctxForInput) WorkerLocalOption(key string) interface{} { return nil }
func (i ctxForInput) PartitionID() string                      { return "0" }
func (i ctxForInput) NumOutputs() int                          { return i.numOutputs }
func (i ctxForInput) AddMetric(name string, delta int)         {}
func (i ctxForInput) SetMetric(name string, val int)           {}
