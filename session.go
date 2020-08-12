package lrmr

import (
	"context"
	"time"

	"github.com/airbloc/logger"
	"github.com/goombaio/namegenerator"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/internal/serialization"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/master"
)

type Session struct {
	ctx        context.Context
	master     *master.Master
	broadcasts serialization.Broadcast
	options    SessionOptions
}

func NewSession(ctx context.Context, m *master.Master, opts ...SessionOption) *Session {
	return &Session{
		ctx:        ctx,
		master:     m,
		broadcasts: make(serialization.Broadcast),
		options:    buildSessionOptions(opts),
	}
}

// Parallelize creates new Dataset from given value.
func (s *Session) Parallelize(val interface{}) *Dataset {
	in := &parallelizedInput{data: lrdd.From(val)}
	return newDataset(s, in)
}

// FromFile creates new Dataset by reading files under given path.
func (s *Session) FromFile(path string) *Dataset {
	in := &localInput{Path: path}
	return newDataset(s, in)
}

// Broadcast shares given value across the cluster. The data broadcasted this way
// is cached in serialized form and deserialized before running each task.
func (s *Session) Broadcast(key string, val interface{}) {
	s.broadcasts[key] = val
}

func (s *Session) Run(ds *Dataset) (*RunningJob, error) {
	timer := log.Timer()

	jobName := s.options.Name
	if jobName == "" {
		jobName = namegenerator.NewNameGenerator(time.Now().UnixNano()).Generate()
	}
	ctx := s.ctx
	if s.options.Timeout > 0 {
		tctx, cancel := context.WithTimeout(ctx, s.options.Timeout)
		ctx = tctx
		defer cancel()
	}

	assignments, j, err := s.master.CreateJob(ctx, jobName, ds.plans, ds.stages, ds.createJobOpts...)
	if err != nil {
		return nil, err
	}
	jobLog := log.WithAttrs(logger.Attrs{"id": j.ID, "job": j.Name})

	broadcast, err := serialization.SerializeBroadcast(s.broadcasts)
	if err != nil {
		return nil, errors.Wrap(err, "serialize broadcast")
	}
	if err := s.master.StartJob(ctx, j, assignments, broadcast); err != nil {
		return nil, errors.WithMessage(err, "assign task")
	}

	jobLog.Verbose("Feeding input")
	iw, err := s.master.OpenInputWriter(ctx, j, j.Stages[1].Name, assignments[1], ds.plans[0].Partitioner)
	if err != nil {
		return nil, errors.WithMessage(err, "open input")
	}
	if err := ds.input.FeedInput(iw); err != nil {
		return nil, errors.Wrap(err, "feed input")
	}
	if err := iw.Close(); err != nil {
		return nil, errors.Wrap(err, "close input")
	}
	timer.End("Job creation completed. Now running...")

	return &RunningJob{
		master: s.master,
		Job:    j,
	}, nil
}
