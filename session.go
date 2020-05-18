package lrmr

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/internal/dynfunc"
	"github.com/therne/lrmr/internal/serialization"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/partitions"
	"github.com/therne/lrmr/transformer"
)

type Session struct {
	input  InputProvider
	stages []*job.Stage

	// len(plans) == len(stages)+1 (because of input stage)
	plans       []partitions.Plan
	defaultPlan partitions.Plan

	broadcasts serialization.Broadcast
}

func NewSession() *Session {
	return &Session{
		broadcasts: make(serialization.Broadcast),
	}
}

func (s *Session) Broadcast(key string, val interface{}) {
	s.broadcasts[key] = val
}

func (s *Session) SetInput(ip InputProvider) *Session {
	s.input = ip
	s.plans = append(s.plans, partitions.Plan{
		Partitioner:  ip,
		DesiredCount: 1,
		MaxNodes:     1,
	})
	return s
}

func (s *Session) AddStage(tf transformer.Transformer, fn interface{}) *Session {
	stageName := fmt.Sprintf("%s%d", tf.Name(), len(s.stages))

	dynFn := dynfunc.From(stageName, fn)
	dynfunc.Register(dynFn)
	if err := tf.SetupAndValidate(dynFn); err != nil {
		panic(stageName + ": " + err.Error())
	}

	st := job.NewStage(stageName, len(s.stages), nil, nil, tf)
	s.stages = append(s.stages, st)
	s.plans = append(s.plans, s.defaultPlan)
	return s
}

func (s *Session) SetPartitioner(p partitions.Partitioner) *Session {
	if len(s.plans) == 0 {
		panic("you need to add stage first.")
	}
	s.plans[len(s.plans)-1].Partitioner = p
	return s
}

func (s *Session) SetDesiredPartitionCount(numCount int) *Session {
	if len(s.plans) == 0 {
		panic("you need to add any stage first.")
	}
	s.plans[len(s.plans)-1].DesiredCount = numCount
	return s
}

func (s *Session) DefaultPlan() *partitions.Plan {
	return &s.defaultPlan
}

func (s *Session) Run(ctx context.Context, name string, m *master.Master) (*RunningJob, error) {
	timer := log.Timer()

	assignments, j, err := m.CreateJob(ctx, name, s.plans, s.stages)
	if err != nil {
		return nil, err
	}
	jobLog := log.WithAttrs(logger.Attrs{"id": j.ID, "job": j.Name})

	broadcast, err := serialization.SerializeBroadcast(s.broadcasts)
	if err != nil {
		return nil, errors.Wrap(err, "serialize broadcast")
	}
	if err := m.StartJob(ctx, j, assignments, broadcast); err != nil {
		return nil, errors.WithMessage(err, "assign task")
	}

	jobLog.Info("Feeding input")
	iw, err := m.OpenInputWriter(ctx, j, assignments[1], s.plans[0].Partitioner)
	if err != nil {
		return nil, errors.WithMessage(err, "open input")
	}
	if err := s.input.FeedInput(iw); err != nil {
		return nil, errors.Wrap(err, "feed input")
	}
	if err := iw.Close(); err != nil {
		return nil, errors.Wrap(err, "close input")
	}
	timer.End("Job creation completed. Now running...")

	return &RunningJob{
		master: m,
		Job:    j,
	}, nil
}
