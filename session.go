package lrmr

import (
	"context"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/job/partitions"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
	"golang.org/x/sync/errgroup"
	"path"
	"strconv"
	"sync"
)

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

	broadcasts stage.SerializedBroadcasts

	log logger.Logger
}

func NewSession(master *master.Master) Session {
	return &session{
		broadcasts: make(map[string][]byte),
		master:     master,
		log:        logger.New("session"),
	}
}

func (s *session) Broadcast(key string, val interface{}) {
	s.broadcasts.Put(key, val)
}

func (s *session) SetInput(ip InputProvider) Session {
	p := job.PartitionPlan{Partitioner: lrmrpb.Output_SHUFFLE}
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
		Partitioner: lrmrpb.Output_PRESERVE,
	})
	s.broadcasts.SerializeStage(st, runner)
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

	s.plans[len(s.plans)-1].Partitioner = lrmrpb.Output_SHUFFLE

	physicalPlans, j, err := s.master.CreateJob(ctx, name, s.plans, s.stages)
	if err != nil {
		return nil, err
	}
	jobLog := s.log.WithAttrs(logger.Attrs{"id": j.ID, "job": j.Name})

	if err := s.master.JobScheduler.AssignTasks(ctx, j, s.plans, physicalPlans, s.broadcasts); err != nil {
		return nil, errors.WithMessage(err, "assign task")
	}

	jobLog.Info("Running input stage")
	if err := s.startInput(ctx, j, physicalPlans[0], s.plans[0].Partitioner); err != nil {
		return nil, errors.WithMessage(err, "input")
	}
	timer.End("Job creation completed. Now running...")
	return &RunningJob{
		master: s.master,
		Job:    j,
	}, nil
}

func (s *session) startInput(ctx context.Context, j *job.Job, targets partitions.PhysicalPlans, partitioner lrmrpb.Output_PartitionerType) error {
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

	var p output.Partitioner
	switch partitioner {
	case lrmrpb.Output_SHUFFLE:
		p = output.NewShuffledPartitioner(len(targets))
	case lrmrpb.Output_HASH_KEY:
		p = output.NewHashKeyPartitioner(len(targets))
	default:
		return errors.Errorf("partitioner %s is unsupported on input stage", partitioner.String())
	}
	out := output.NewWriter(p, outs)
	err := s.input.ProvideInput(out)
	if closeErr := out.Close(); err == nil {
		return closeErr
	}
	return err
}
