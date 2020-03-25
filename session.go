package lrmr

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/shamaton/msgpack"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
	"path"
)

type Result struct {
	Ok     bool
	Errors []error
	Output interface{}
}

type Session interface {
	Broadcast(key string, val interface{})
	SetInput(ip InputProvider) Session
	AddStage(stage.Stage, interface{}) Session
	Output(out *job.StageOutput) Session

	Run(ctx context.Context, name string) (*RunningJob, error)
}

type session struct {
	master *Master
	input  InputProvider
	stages []*job.Stage

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
	defaultOut := job.DescribingStageOutput().NoPartition()
	name := fmt.Sprintf("%s%d", st.Name, len(s.stages))
	s.stages = append(s.stages, job.NewStage(name, st.Name, defaultOut))

	data := st.Serialize(box)
	s.broadcasts["__stage/"+name] = data
	s.serializedBroadcasts["__stage/"+name] = data
	return s
}

func (s *session) Run(ctx context.Context, name string) (*RunningJob, error) {
	defer s.log.Recover()

	j, err := s.master.jobManager.CreateJob(ctx, name, s.stages)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}
	s.master.jobTracker.AddJob(j)

	jobLog := s.log.WithAttrs(logger.Attrs{"job": j.ID, "jobName": j.Name})

	workerConns := make(map[string]lrmrpb.WorkerClient, len(j.Workers))
	for _, worker := range j.Workers {
		conn, err := s.master.nodeManager.Connect(ctx, worker.Host)
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", worker.Host, err)
		}
		workerConns[worker.Host] = lrmrpb.NewWorkerClient(conn)
	}

	partitions := make([]*lrmrpb.Output, len(j.Stages)+1)
	for i, st := range j.Stages {
		var nextStageName string
		if i == len(j.Stages)-1 {
			nextStageName = "__none"
		} else {
			nextStageName = j.Stages[i+1].Name
		}
		partitions[i+1] = st.Output.Build(nextStageName, st.Workers)
	}
	// TODO: set input stage partitioner
	partitions[0] = job.DescribingStageOutput().Build(j.Stages[0].Name, j.Stages[0].Workers)

	// initialize tasks reversely, so that outputs can be connected with next stage
	prevPartitions := &lrmrpb.Output{
		Type:            lrmrpb.Output_PUSH,
		Partitioner:     lrmrpb.Output_SHUFFLE,
		PartitionToHost: map[string]string{},
		StageName:       "__none",
	}
	if j.Stages[len(j.Stages)-1].Output.Collect {
		prevPartitions.PartitionToHost["0"] = s.master.node.Host
		prevPartitions.StageName = "__collect"
	}
	for i := len(j.Stages) - 1; i >= 0; i-- {
		curStage := j.Stages[i]

		for key, host := range partitions[i].PartitionToHost {
			w := &node.Node{Host: host}
			req := &lrmrpb.CreateTaskRequest{
				Stage: &lrmrpb.Stage{
					JobID:      j.ID,
					Name:       curStage.Name,
					RunnerName: curStage.RunnerName,
				},
				PartitionKey: key,
				Output:       prevPartitions,
				Broadcasts:   s.serializedBroadcasts,
			}
			if err := s.createTask(w, req); err != nil {
				return nil, errors.Wrapf(err, "create task for %s/%s/%s on %s", j.Name, curStage.Name, key, host)
			}
		}
		prevPartitions = partitions[i]
	}

	jobLog.Info("Starting input")
	outs := make(map[string]output.Output)
	for key, host := range prevPartitions.PartitionToHost {
		taskID := path.Join(j.ID, prevPartitions.StageName, key)
		out, err := output.NewPushStream(ctx, s.master.nodeManager, host, taskID)
		if err != nil {
			return nil, errors.Wrapf(err, "connect %s for input", host)
		}
		outs[key] = out
	}
	out := output.NewWriter(output.NewShuffledPartitioner(len(outs)), outs)

	if err := s.input.ProvideInput(out); err != nil {
		return nil, fmt.Errorf("running input: %w", err)
	}
	if err := out.Close(); err != nil {
		return nil, fmt.Errorf("close input: %w", err)
	}
	jobLog.Info("Finished providing input. Running...")
	return &RunningJob{
		master: s.master,
		Job:    j,
	}, nil
}

func (s *session) createTask(worker *node.Node, req *lrmrpb.CreateTaskRequest) error {
	conn, err := s.master.nodeManager.Connect(context.TODO(), worker.Host)
	if err != nil {
		return errors.Wrap(err, "grpc dial")
	}
	_, err = lrmrpb.NewWorkerClient(conn).CreateTask(context.TODO(), req)
	return err
}

// Output sets last stage output with given output spec.
func (s *session) Output(out *job.StageOutput) Session {
	s.stages[len(s.stages)-1].Output = out
	return s
}
