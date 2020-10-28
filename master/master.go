package master

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/cluster/node"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/internal/pbtypes"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/partitions"
	"github.com/therne/lrmr/stage"
	"github.com/therne/lrmr/transformation"
	"github.com/therne/lrmr/worker"
	"github.com/thoas/go-funk"
	"golang.org/x/sync/errgroup"
)

var ErrNoAvailableWorkers = errors.New("no available workers")

var log = logger.New("lrmr")

type Master struct {
	executor *worker.Worker

	ClusterStates coordinator.Coordinator
	JobManager    *job.Manager
	JobTracker    *job.Tracker
	JobReporter   *job.Reporter
	NodeManager   node.Manager

	opt Options
}

func New(crd coordinator.Coordinator, opt Options) (*Master, error) {
	// create task executor by running worker
	wopt := worker.DefaultOptions()
	wopt.NodeType = node.Master
	wopt.ListenHost = opt.ListenHost
	wopt.AdvertisedHost = opt.AdvertisedHost
	wopt.Input.MaxRecvSize = opt.Input.MaxRecvSize
	wopt.Output.BufferLength = opt.Output.BufferLength
	wopt.Output.MaxSendMsgSize = opt.Output.MaxSendMsgSize
	w, err := worker.New(crd, wopt)
	if err != nil {
		return nil, errors.Wrap(err, "init master task executor")
	}

	jm := job.NewManager(w.NodeManager, crd)
	return &Master{
		executor:      w,
		ClusterStates: crd,
		JobManager:    jm,
		JobTracker:    job.NewJobTracker(crd, jm),
		JobReporter:   job.NewJobReporter(crd),
		NodeManager:   w.NodeManager,
		opt:           opt,
	}, nil
}

func (m *Master) Start() {
	go func() {
		if err := m.executor.Start(); err != nil {
			log.Error("Failed to start master task executor", err)
		}
	}()
	go m.JobTracker.HandleJobCompletion()
}

func (m *Master) Workers() ([]WorkerHolder, error) {
	workers, err := m.NodeManager.List(context.TODO(), node.Worker)
	if err != nil {
		return nil, errors.WithMessage(err, "list available workers")
	}
	wh := make([]WorkerHolder, len(workers))
	for i, w := range workers {
		wh[i] = WorkerHolder{
			Node: w,
			nm:   m.NodeManager,
		}
	}
	return wh, nil
}

func (m *Master) State() coordinator.KV {
	return m.NodeManager.NodeStates()
}

func (m *Master) CreateJob(ctx context.Context, name string, plans []partitions.Plan, stages []stage.Stage, opt ...CreateJobOption) ([]partitions.Assignments, *job.Job, error) {
	opts := buildCreateJobOptions(opt)

	workers, err := m.NodeManager.List(ctx, node.Worker)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "list available workers")
	}
	if opts.NodeSelector != nil {
		workers = funk.Filter(
			workers,
			func(n *node.Node) bool { return n.TagMatches(opts.NodeSelector) },
		).([]*node.Node)
	}
	if len(workers) == 0 {
		return nil, nil, ErrNoAvailableWorkers
	}

	pp, assignments := partitions.Schedule(workers, plans, partitions.WithMaster(m.executor.NodeManager.Self()))
	for i, p := range pp {
		stages[i].Output.Partitioner = p.Partitioner

		partitionerName := fmt.Sprintf("%T", partitions.UnwrapPartitioner(p.Partitioner))
		log.Verbose("Planned {} partitions on {}/{} (output with {}):\n{}", len(p.Partitions),
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
	prepareCollect(j.ID)

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
		} else {
			reqTmpl.Output.PartitionToHost = make(map[string]string, 0)
		}

		t := log.Timer()
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
		t.End("Initialized stage {}/{}", j.ID, s.Name)
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
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultChan, err := getCollectedResultChan(jobID)
	if err != nil {
		return nil, err
	}
	select {
	case result := <-resultChan:
		return result, nil

	case err := <-m.JobManager.WatchJobErrors(watchCtx, jobID):
		return nil, err
	}
}

func (m *Master) Stop() {
	m.JobTracker.Close()
	if err := m.executor.Close(); err != nil {
		log.Error("failed to close worker")
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
