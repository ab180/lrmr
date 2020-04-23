package lrmr

import (
	"context"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/shamaton/msgpack"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/job/partitions"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/master"
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
	master *master.Master
	input  InputProvider
	stages []*job.Stage

	// len(plans) == len(stages)+1
	plans []job.PartitionPlan

	broadcasts           map[string]interface{}
	serializedBroadcasts map[string][]byte

	log logger.Logger
}

func NewSession(master *master.Master) Session {
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
	p := job.PartitionPlan{Partitioner: lrmrpb.Output_PRESERVE}
	if planner, ok := ip.(partitions.LogicalPlanner); ok {
		p.PlanOptions = append(p.PlanOptions, partitions.WithLogicalPlanner(planner))
	}
	s.input = ip
	s.plans = append(s.plans, p)
	return s
}

func (s *session) AddStage(runner interface{}) Session {
	st := stage.LookupByRunner(runner)
	s.stages = append(s.stages, job.NewStage(st.Name+strconv.Itoa(len(s.stages)), st.Name))
	s.plans = append(s.plans, job.PartitionPlan{
		Partitioner: lrmrpb.Output_SHUFFLE,
	})

	data := st.Serialize(runner)
	s.broadcasts["__stage/"+st.Name] = data
	s.serializedBroadcasts["__stage/"+st.Name] = data
	return s
}

func (s *session) SetPartitionOption(opts ...partitions.PlanOption) Session {
	if len(s.plans) == 0 {
		panic("you need to add stage first.")
	}
	s.plans[len(s.plans)-1].PlanOptions = opts
	return s
}

func (s *session) SetPartitionType(partitioner lrmrpb.Output_PartitionerType) Session {
	if len(s.plans) == 0 {
		panic("you need to add stage first.")
	}
	s.plans[len(s.plans)-1].Partitioner = partitioner
	return s
}

func (s *session) Run(ctx context.Context, name string) (_ *RunningJob, err error) {
	defer func() {
		if p := logger.WrapRecover(recover()); p != nil {
			err = p
		}
	}()
	timer := log.Timer()

	workers, err := s.master.NodeManager.List(ctx, node.Worker)
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

	physicalPlans := make([]partitions.PhysicalPlans, len(s.plans))
	for i, p := range s.plans {
		if i == len(s.stages) {
			p.PlanOptions = []partitions.PlanOption{partitions.WithEmpty()}
		}
		logical, physical := sched.Plan(p.PlanOptions...)

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
		s.log.Verbose("Planned {} partitions on {}/{}:\n{}",
			len(physical), name, stageName, physical.Pretty())
	}

	j, err := s.master.JobManager.CreateJob(ctx, name, s.stages)
	if err != nil {
		return nil, errors.WithMessage(err, "create job")
	}
	s.master.JobTracker.AddJob(j)
	jobLog := s.log.WithAttrs(logger.Attrs{"id": j.ID, "job": j.Name})

	if err := s.master.JobScheduler.AssignTasks(ctx, j, s.plans, physicalPlans, s.serializedBroadcasts); err != nil {
		return nil, errors.WithMessage(err, "assign task")
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

func (s *session) startInput(ctx context.Context, j *job.Job, targets []partitions.PhysicalPlan) error {
	outs := make(map[string]output.Output)
	var lock sync.Mutex

	wg, reqCtx := errgroup.WithContext(ctx)
	for _, t := range targets {
		p := t
		wg.Go(func() error {
			taskID := path.Join(j.ID, j.Stages[0].Name, p.Key)
			out, err := output.NewPushStream(reqCtx, s.master.NodeManager, p.Node.Host, taskID)
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
