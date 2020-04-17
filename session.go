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
	"strconv"
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
	AddStage(interface{}) Session
	SetPartitionOption(...partitions.PlanOption) Session
	SetPartitionType(lrmrpb.Output_PartitionerType) Session

	Run(ctx context.Context, name string) (*RunningJob, error)
}

type session struct {
	master *Master
	input  InputProvider
	stages []*job.Stage

	// len(plans) == len(stages)+1
	partitions []partitionDef

	broadcasts           map[string]interface{}
	serializedBroadcasts map[string][]byte

	log logger.Logger
}

type partitionDef struct {
	partitioner lrmrpb.Output_PartitionerType
	planOpts    []partitions.PlanOption
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
	p := partitionDef{partitioner: lrmrpb.Output_PRESERVE}
	if planner, ok := ip.(partitions.LogicalPlanner); ok {
		p.planOpts = append(p.planOpts, partitions.WithLogicalPlanner(planner))
	}
	s.input = ip
	s.partitions = append(s.partitions, p)
	return s
}

func (s *session) AddStage(runner interface{}) Session {
	st := stage.LookupByRunner(runner)
	s.stages = append(s.stages, job.NewStage(st.Name+strconv.Itoa(len(s.stages)), st.Name))
	s.partitions = append(s.partitions, partitionDef{partitioner: lrmrpb.Output_SHUFFLE})

	data := st.Serialize(runner)
	s.broadcasts["__stage/"+st.Name] = data
	s.serializedBroadcasts["__stage/"+st.Name] = data
	return s
}

func (s *session) SetPartitionOption(opts ...partitions.PlanOption) Session {
	if len(s.partitions) == 0 {
		panic("you need to add stage first.")
	}
	s.partitions[len(s.partitions)-1].planOpts = opts
	return s
}

func (s *session) SetPartitionType(partitioner lrmrpb.Output_PartitionerType) Session {
	if len(s.partitions) == 0 {
		panic("you need to add stage first.")
	}
	s.partitions[len(s.partitions)-1].partitioner = partitioner
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

	physicalPlans := make([]partitions.PhysicalPlans, len(s.partitions))
	for i, p := range s.partitions {
		if i == len(s.stages) {
			p.planOpts = []partitions.PlanOption{partitions.WithEmpty()}
		}
		log.Verbose("Options: {}", p.planOpts)
		logical, physical := sched.Plan(p.planOpts...)

		physicalPlans[i] = physical
		if i > 0 {
			s.stages[i-1].Partitions = logical
		}
		var stageName string
		if i < len(s.stages) {
			stageName = s.stages[i].Name
		} else {
			stageName = "__final"
		}
		log.Verbose("Planned {} partitions on {}/{}:\n{}", len(physical), name, stageName, physical.Pretty())
	}

	j, err := s.master.jobManager.CreateJob(ctx, name, s.stages)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}
	s.master.jobTracker.AddJob(j)
	jobLog := s.log.WithAttrs(logger.Attrs{"id": j.ID, "job": j.Name})

	// initialize tasks reversely, so that outputs can be connected with next stage
	for i := len(j.Stages) - 1; i >= 0; i-- {
		wg, reqCtx := errgroup.WithContext(ctx)
		for _, curPartition := range physicalPlans[i] {
			w := &node.Node{Host: curPartition.Node.Host}
			req := &lrmrpb.CreateTaskRequest{
				JobID:      j.ID,
				StageName:  j.Stages[i].Name,
				Input:      s.buildInputAt(i, curPartition),
				Output:     s.buildOutputTo(i+1, physicalPlans[i+1]),
				Broadcasts: s.serializedBroadcasts,
			}
			wg.Go(func() error {
				if err := s.createTask(reqCtx, w, req); err != nil {
					return errors.Wrapf(err, "create task for %s/%s/%s on %s", req.JobID, req.StageName, req.Input.PartitionKey, w.Host)
				}
				return nil
			})
		}
		if err := wg.Wait(); err != nil {
			return nil, err
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

func (s *session) buildInputAt(stageIdx int, curPartition partitions.PhysicalPlan) *lrmrpb.Input {
	var prevStageName string
	if stageIdx == 0 {
		prevStageName = "__input"
	} else {
		prevStageName = s.stages[stageIdx-1].Name
	}
	return &lrmrpb.Input{
		Type:          lrmrpb.Input_PUSH,
		PartitionKey:  curPartition.Key,
		PrevStageName: prevStageName,
	}
}

func (s *session) buildOutputTo(nextStageIdx int, nextPartitions partitions.PhysicalPlans) *lrmrpb.Output {
	var nextStageName string
	if nextStageIdx < len(s.stages) {
		nextStageName = s.stages[nextStageIdx].Name
	} else {
		nextStageName = "__final"
	}
	return &lrmrpb.Output{
		Type:            lrmrpb.Output_PUSH,
		Partitioner:     s.partitions[nextStageIdx].partitioner,
		PartitionToHost: nextPartitions.ToMap(),
		NextStageName:   nextStageName,
	}
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
