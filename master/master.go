package master

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/internal/pbtypes"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/partitions"
	"github.com/therne/lrmr/stage"
	"github.com/therne/lrmr/transformation"
	"golang.org/x/sync/errgroup"
)

var ErrNoAvailableWorkers = errors.New("no available workers")

var log = logger.New("lrmr")

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
	jm := job.NewManager(nm, crd)
	m := &Master{
		JobManager:  jm,
		JobTracker:  job.NewJobTracker(crd, jm),
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

func (m *Master) CreateJob(ctx context.Context, name string, plans []partitions.Plan, stages []stage.Stage) ([]partitions.Assignments, *job.Job, error) {
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
	colStage := stage.New(CollectStageName, nil)
	stages[len(stages)-1].SetOutputTo(colStage)
	stages = append(stages, colStage)

	pp, assignments := partitions.Schedule(workers, plans, partitions.WithMaster(m.collector.Node))
	for i, p := range pp {
		stages[i].Output.Partitioner = p.Partitioner

		partitionerName := fmt.Sprintf("%T", partitions.UnwrapPartitioner(p.Partitioner))
		log.Verbose("Planned {} partitions on {}/{} (output by {}):\n{}", len(p.Partitions),
			name, stages[i].Name, partitionerName, assignments[i].Pretty())
	}

	j, err := m.JobManager.CreateJob(ctx, name, stages)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "create job")
	}
	m.JobTracker.AddJob(j)

	return assignments, j, nil
}

// StartTasks create tasks to the nodes with the plan.
func (m *Master) StartJob(ctx context.Context, j *job.Job, assignments []partitions.Assignments, broadcasts map[string][]byte) error {
	m.collector.Prepare(j.ID)

	// initialize tasks reversely, so that outputs can be connected with next stage
	for i := len(j.Stages) - 1; i >= 1; i-- {
		s := j.Stages[i]
		reqTmpl := lrmrpb.CreateTasksRequest{
			Job: &lrmrpb.Job{
				Id:   j.ID,
				Name: j.Name,
			},
			Stage: pbtypes.MustMarshalJSON(s),
			Input: []*lrmrpb.Input{
				{Type: lrmrpb.Input_PUSH},
			},
			Output: &lrmrpb.Output{
				Type: lrmrpb.Output_PUSH,
			},
			Broadcasts: broadcasts,
		}
		if i < len(j.Stages)-1 {
			reqTmpl.Output.PartitionToHost = assignments[i+1].ToMap()
		}

		wg, wctx := errgroup.WithContext(ctx)
		for h, ps := range assignments[i].GroupIDsByHost() {
			host := h
			partitionIDs := ps

			wg.Go(func() error {
				conn, err := m.NodeManager.Connect(wctx, host)
				if err != nil {
					return errors.Wrapf(err, "dial %s for stage %s", host, s.Name)
				}
				req := reqTmpl
				req.PartitionIDs = partitionIDs
				if _, err := lrmrpb.NewNodeClient(conn).CreateTasks(wctx, &req); err != nil {
					return errors.Wrapf(err, "call CreateTask on %s", host)
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

func (m *Master) OpenInputWriter(ctx context.Context, j *job.Job, stageName string, targets partitions.Assignments, partitioner partitions.Partitioner) (output.Output, error) {
	outs := make(map[string]output.Output, len(targets))
	var lock sync.Mutex

	wg, reqCtx := errgroup.WithContext(ctx)
	for _, t := range targets {
		p := t
		wg.Go(func() error {
			taskID := path.Join(j.ID, stageName, p.ID)
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
	out := output.NewWriter(newContextForInput(ctx, len(targets)), partitioner, outs)
	return out, nil
}

func (m *Master) CollectedResults(jobID string) ([]*lrdd.Row, error) {
	result := <-m.collector.Collect(jobID)
	if result.Error != nil {
		return nil, result.Error
	}
	return result.Result, nil
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
func (i ctxForInput) JobID() string                            { return "" }
func (i ctxForInput) NumOutputs() int                          { return i.numOutputs }
func (i ctxForInput) AddMetric(name string, delta int)         {}
func (i ctxForInput) SetMetric(name string, val int)           {}
