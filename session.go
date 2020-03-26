package lrmr

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/shamaton/msgpack"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/job/partitions"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
	"golang.org/x/sync/errgroup"
	"path"
	"sync"
)

var ErrNoAvailableWorkers = errors.New("no available workers")

type Result struct {
	Ok     bool
	Errors []error
	Output interface{}
}

type Session interface {
	Broadcast(key string, val interface{})
	SetInput(ip InputProvider) Session
	AddStage(stage.Stage, interface{}) Session
	SetPartitionOption(...partitions.PlanOptions) Session
	SetPartitionType(lrmrpb.Output_PartitionerType) Session

	Run(ctx context.Context, name string) (*RunningJob, error)
}

type session struct {
	master *Master
	input  InputProvider
	stages []*job.Stage

	partitioners []lrmrpb.Output_PartitionerType
	planOpts     [][]partitions.PlanOptions

	broadcasts           map[string]interface{}
	serializedBroadcasts map[string][]byte

	log logger.Logger
}

func NewSession(master *Master) Session {
	return &session{
		broadcasts:           make(map[string]interface{}),
		serializedBroadcasts: make(map[string][]byte),
		master:               master,
		log:                  logger.New("session"),
	}
}

func (s *session) Broadcast(key string, val interface{}) {
	sv, err := msgpack.Encode(val)
	if err != nil {
		panic("broadcast value must be serializable: " + err.Error())
	}
	s.serializedBroadcasts["Broadcast/"+key] = sv
	s.broadcasts["Broadcast/"+key] = val
}

func (s *session) SetInput(ip InputProvider) Session {
	s.input = ip
	return s
}

func (s *session) AddStage(st stage.Stage, box interface{}) Session {
	name := fmt.Sprintf("%s%d", st.Name, len(s.stages))
	s.stages = append(s.stages, job.NewStage(name, st.Name))
	s.planOpts = append(s.planOpts, nil)
	s.partitioners = append(s.partitioners, lrmrpb.Output_SHUFFLE)

	data := st.Serialize(box)
	s.broadcasts["__stage/"+name] = data
	s.serializedBroadcasts["__stage/"+name] = data
	return s
}

func (s *session) SetPartitionOption(opts ...partitions.PlanOptions) Session {
	if len(s.stages) == 0 {
		panic("you need to add stage first.")
	}
	s.planOpts[len(s.stages)-1] = opts
	return s
}

func (s *session) SetPartitionType(partitioner lrmrpb.Output_PartitionerType) Session {
	if len(s.stages) == 0 {
		panic("you need to add stage first.")
	}
	s.partitioners[len(s.stages)-1] = partitioner
	return s
}

func (s *session) Run(ctx context.Context, name string) (_ *RunningJob, err error) {
	defer func() {
		if p := logger.WrapRecover(recover()); p != nil {
			err = p
		}
	}()
	timer := log.Timer()

	workers, err := s.master.nodeManager.List(ctx, node.Worker)
	if err != nil {
		return nil, errors.WithMessage(err, "list available workers")
	}
	if len(workers) == 0 {
		return nil, ErrNoAvailableWorkers
	}
	sched, err := partitions.NewSchedulerWithNodes(ctx, nil, workers)
	if err != nil {
		return nil, errors.WithMessage(err, "start scheduling")
	}

	physicalPlans := make([]partitions.PhysicalPlans, len(s.stages))
	for i, planOpts := range s.planOpts {
		logical, physical := sched.Plan(planOpts...)

		s.stages[i].Partitions = logical
		physicalPlans[i] = physical

		log.Verbose("Planned {} partitions on {}/{}:\n{}", len(physical), name, s.stages[i].Name, physical.Pretty())
	}

	j, err := s.master.jobManager.CreateJob(ctx, name, s.stages)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}
	s.master.jobTracker.AddJob(j)
	jobLog := s.log.WithAttrs(logger.Attrs{"id": j.ID, "job": j.Name})

	// initialize tasks reversely, so that outputs can be connected with next stage
	nextOutput := new(lrmrpb.Output)
	for i := len(j.Stages) - 1; i >= 0; i-- {
		curStage := j.Stages[i]
		curPartitions := physicalPlans[i]

		wg, reqCtx := errgroup.WithContext(ctx)
		for _, p := range curPartitions {
			w := &node.Node{Host: p.Node.Host}
			req := &lrmrpb.CreateTaskRequest{
				PartitionKey: p.Key,
				Stage: &lrmrpb.Stage{
					JobID:      j.ID,
					Name:       curStage.Name,
					RunnerName: curStage.RunnerName,
				},
				Output:     nextOutput,
				Broadcasts: s.serializedBroadcasts,
			}
			wg.Go(func() error {
				if err := s.createTask(reqCtx, w, req); err != nil {
					taskID := path.Join(req.Stage.JobID, req.Stage.Name, req.PartitionKey)
					return errors.Wrapf(err, "create task for %s on %s", taskID, w.Host)
				}
				return nil
			})
		}
		if err := wg.Wait(); err != nil {
			return nil, err
		}
		nextOutput = &lrmrpb.Output{
			Type:            lrmrpb.Output_PUSH,
			Partitioner:     s.partitioners[i],
			PartitionToHost: curPartitions.ToMap(),
			StageName:       curStage.Name,
		}
	}

	jobLog.Info("Running input stage")
	if err := s.startInput(ctx, j, physicalPlans[0]); err != nil {
		return nil, errors.WithMessage(err, "input")
	}
	timer.End("Job creation completed. Now running...")
	return &RunningJob{
		master: s.master,
		Job:    j,
	}, nil
}

func (s *session) createTask(ctx context.Context, worker *node.Node, req *lrmrpb.CreateTaskRequest) error {
	conn, err := s.master.nodeManager.Connect(ctx, worker.Host)
	if err != nil {
		return errors.Wrap(err, "grpc dial")
	}
	_, err = lrmrpb.NewWorkerClient(conn).CreateTask(context.TODO(), req)
	return err
}

func (s *session) startInput(ctx context.Context, j *job.Job, targets []partitions.PhysicalPlan) error {
	outs := make(map[string]output.Output)
	var lock sync.Mutex

	wg, reqCtx := errgroup.WithContext(ctx)
	for _, t := range targets {
		p := t
		wg.Go(func() error {
			taskID := path.Join(j.ID, j.Stages[0].Name, p.Key)
			out, err := output.NewPushStream(reqCtx, s.master.nodeManager, p.Node.Host, taskID)
			if err != nil {
				return errors.Wrapf(err, "connect %s", p.Node.Host)
			}
			lock.Lock()
			outs[p.Key] = out
			lock.Unlock()
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return err
	}
	out := output.NewWriter(output.NewShuffledPartitioner(len(targets)), outs)
	err := s.input.ProvideInput(out)
	if closeErr := out.Close(); err == nil {
		return closeErr
	}
	return err
}
