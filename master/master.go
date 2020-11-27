package master

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/internal/pbtypes"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/lrmrpb"
	"github.com/ab180/lrmr/output"
	"github.com/ab180/lrmr/partitions"
	"github.com/ab180/lrmr/stage"
	"github.com/ab180/lrmr/worker"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var ErrNoAvailableWorkers = errors.New("no available workers")

var log = logger.New("lrmr")

type Master struct {
	executor *worker.Worker

	Node       *node.Node
	Cluster    cluster.Cluster
	JobManager *job.Manager
	JobTracker *job.Tracker

	opt Options
}

func New(crd coordinator.Coordinator, opt Options) (*Master, error) {
	c, err := cluster.OpenRemote(crd, cluster.DefaultOptions())
	if err != nil {
		return nil, err
	}

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

	jm := job.NewManager(crd)
	return &Master{
		executor:   w,
		Cluster:    c,
		JobManager: jm,
		JobTracker: job.NewJobTracker(crd, jm),
		opt:        opt,
	}, nil
}

func (m *Master) Start() {
	go func() {
		if err := m.executor.Start(); err != nil {
			log.Error("Failed to start master task executor", err)
		}
	}()
}

func (m *Master) Workers() ([]WorkerHolder, error) {
	workers, err := m.Cluster.List(context.TODO(), cluster.ListOption{Type: node.Worker})
	if err != nil {
		return nil, errors.WithMessage(err, "list available workers")
	}
	wh := make([]WorkerHolder, len(workers))
	for i, w := range workers {
		wh[i] = WorkerHolder{
			Node:    w,
			cluster: m.Cluster,
		}
	}
	return wh, nil
}

func (m *Master) CreateJob(ctx context.Context, name string, plans []partitions.Plan, stages []stage.Stage, opt ...CreateJobOption) (*job.Job, error) {
	opts := buildCreateJobOptions(opt)

	listOpts := cluster.ListOption{Type: node.Worker}
	if opts.NodeSelector != nil {
		listOpts.Tag = opts.NodeSelector
	}
	workers, err := m.Cluster.List(ctx, listOpts)
	if err != nil {
		return nil, errors.WithMessage(err, "list available workers")
	}
	if len(workers) == 0 {
		return nil, ErrNoAvailableWorkers
	}

	pp, assignments := partitions.Schedule(workers, plans, partitions.WithMaster(m.executor.Node.Info()))
	for i, p := range pp {
		stages[i].Output.Partitioner = p.Partitioner

		partitionerName := fmt.Sprintf("%T", partitions.UnwrapPartitioner(p.Partitioner))
		log.Verbose("Planned {} partitions on {}/{} (output with {}):\n{}", len(p.Partitions),
			name, stages[i].Name, partitionerName, assignments[i].Pretty())
	}

	j, err := m.JobManager.CreateJob(ctx, name, stages, assignments)
	if err != nil {
		return nil, errors.WithMessage(err, "create job")
	}
	// m.JobTracker.OnTaskCompletion(j, func(j *job.Job, stageName string, doneCountInStage int) {
	// 	totalTasks := len(j.GetPartitionsOfStage(stageName))
	// 	log.Verbose("Task ({}/{}) finished of {}/{}", doneCountInStage, totalTasks, j.ID, stageName)
	// })
	m.JobTracker.OnStageCompletion(j, func(j *job.Job, stageName string, stageStatus *job.StageStatus) {
		log.Verbose("Stage {}/{} {}.", j.ID, stageName, stageStatus.Status)
	})
	m.JobTracker.OnJobCompletion(j, func(j *job.Job, status *job.Status) {
		log.Info("Job {} {}. Total elapsed {}", j.ID, status.Status, time.Since(j.SubmittedAt))
		for i, errDesc := range status.Errors {
			log.Info(" - Error #{} caused by {}: {}", i, errDesc.Task, errDesc.Message)
		}
	})
	return j, nil
}

// StartTasks create tasks to the nodes with the plan.
func (m *Master) StartJob(ctx context.Context, j *job.Job, broadcasts map[string][]byte) error {
	prepareCollect(j.ID)
	marshalledJob := pbtypes.MustMarshalJSON(j)

	// initialize tasks reversely, so that outputs can be connected with next stage
	for i := len(j.Stages) - 1; i >= 1; i-- {
		s := j.Stages[i]
		reqTmpl := lrmrpb.CreateTasksRequest{
			Job:   marshalledJob,
			Stage: s.Name,
			Input: []*lrmrpb.Input{
				{Type: lrmrpb.Input_PUSH},
			},
			Output: &lrmrpb.Output{
				Type: lrmrpb.Output_PUSH,
			},
			Broadcasts: broadcasts,
		}
		if i < len(j.Stages)-1 {
			reqTmpl.Output.PartitionToHost = j.Partitions[i+1].ToMap()
		} else {
			reqTmpl.Output.PartitionToHost = make(map[string]string, 0)
		}

		t := log.Timer()
		wg, wctx := errgroup.WithContext(ctx)
		for h, ps := range j.Partitions[i].GroupIDsByHost() {
			host, partitionIDs := h, ps

			wg.Go(func() error {
				conn, err := m.Cluster.Connect(wctx, host)
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

func (m *Master) OpenInputWriter(ctx context.Context, j *job.Job, stageName string, input partitions.Partitioner) (output.Output, error) {
	targets := j.GetPartitionsOfStage(stageName)
	outs := make(map[string]output.Output, len(targets))
	var lock sync.Mutex

	jobCtx, cancelJobCtx := context.WithCancel(context.Background())
	m.JobTracker.OnJobCompletion(j, func(*job.Job, *job.Status) {
		cancelJobCtx()
	})

	var wg errgroup.Group
	for _, t := range targets {
		assigned := t
		wg.Go(func() error {
			taskID := path.Join(j.ID, stageName, assigned.PartitionID)
			out, err := output.OpenPushStream(jobCtx, m.Cluster, m.Node, assigned.Host, taskID)
			if err != nil {
				return errors.Wrapf(err, "connect %s", assigned.Host)
			}
			lock.Lock()
			outs[assigned.PartitionID] = out
			lock.Unlock()
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	out := output.NewWriter("0", input, outs)
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
	if err := m.executor.Close(); err != nil {
		log.Error("failed to close worker")
	}
	m.JobTracker.Close()
	if err := m.Cluster.Close(); err != nil {
		log.Error("Failed to close connections to cluster", err)
	}
}
