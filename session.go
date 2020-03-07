package lrmr

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/shamaton/msgpack"
	"github.com/therne/lrmr/internal/logutils"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
)

type Result struct {
	Ok     bool
	Errors []error
	Output interface{}
}

type Session interface {
	Broadcast(key string, val interface{})
	SetInput(ip InputProvider) Session
	AddStage(runnerName string, runner stage.Runner) Session
	Output(out *job.StageOutput) Session

	Run(ctx context.Context, name string) (*RunningJob, error)
}

type session struct {
	master  *Master
	input   InputProvider
	stages  []*job.Stage
	runners []stage.Runner

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

func (s *session) AddStage(runnerName string, runner stage.Runner) Session {
	defaultOut := job.DescribingStageOutput().WithFanout()
	name := fmt.Sprintf("%s%d", runnerName, len(s.stages))
	s.stages = append(s.stages, job.NewStage(name, runnerName, defaultOut))
	s.runners = append(s.runners, runner)

	data, err := msgpack.Encode(runner)
	if err != nil {
		panic(fmt.Sprintf("broadcasting %s: %v", name, err))
	}
	s.broadcasts["__stage/"+name] = data
	s.serializedBroadcasts["__stage/"+name] = data
	return s
}

func (s *session) Run(ctx context.Context, name string) (*RunningJob, error) {
	defer func() {
		if err := logutils.WrapRecover(recover()); err != nil {
			s.log.Error("running session: {}", err.Pretty())
		}
	}()
	job, err := s.master.jobManager.CreateJob(ctx, name, s.stages)
	if err != nil {
		return nil, fmt.Errorf("create job: %w", err)
	}
	s.master.jobTracker.AddJob(job)

	jobLog := s.log.WithAttrs(logger.Attrs{"job": job.ID, "jobName": job.Name})

	workerConns := make(map[string]lrmrpb.WorkerClient, len(job.Workers))
	for _, worker := range job.Workers {
		conn, err := s.master.nodeManager.Connect(ctx, worker)
		if err != nil {
			return nil, fmt.Errorf("dial %s: %w", worker.Host, err)
		}
		workerConns[worker.Host] = lrmrpb.NewWorkerClient(conn)
		defer conn.Close()
	}

	// initialize tasks reversely, so that outputs can be connected with next stage
	var prevShards []*lrmrpb.HostMapping
	lastStage := job.Stages[len(job.Stages)-1]
	if lastStage.Output.Collect {
		prevShards = append(prevShards, &lrmrpb.HostMapping{
			Host:   s.master.node.Host,
			TaskID: "master",
		})
	}
	for i := len(job.Stages) - 1; i >= 0; i-- {
		stage := job.Stages[i]
		req := &lrmrpb.CreateTaskRequest{
			Stage: &lrmrpb.Stage{
				JobID:      job.ID,
				Name:       stage.Name,
				RunnerName: stage.RunnerName,
			},
			Output:     stage.Output.Build(prevShards),
			Broadcasts: s.serializedBroadcasts,
		}
		prevShards = make([]*lrmrpb.HostMapping, len(stage.Workers))
		for j, worker := range stage.Workers {
			taskID, err := s.createTask(worker, req)
			if err != nil {
				return nil, fmt.Errorf("create task for stage %s on %s: %w", stage.Name, worker.Host, err)
			}
			prevShards[j] = &lrmrpb.HostMapping{
				Host:   worker.Host,
				TaskID: taskID,
			}
		}
	}

	jobLog.Info("Starting input")
	outDesc := &lrmrpb.Output{
		Shards:      prevShards,
		Partitioner: &lrmrpb.Partitioner{Type: lrmrpb.Partitioner_NONE},
	}
	firstShards, err := output.DialShards(ctx, s.master.node, outDesc, s.master.opt.Master.Output)
	if err != nil {
		return nil, fmt.Errorf("connecting input: %w", err)
	}
	out := output.NewStreamWriter(firstShards)
	if err := s.input.ProvideInput(out); err != nil {
		return nil, fmt.Errorf("running input: %w", err)
	}
	if err := out.Flush(); err != nil {
		return nil, fmt.Errorf("flushing input: %w", err)
	}
	go func() {
		if err := firstShards.Close(); err != nil {
			jobLog.Error("Failed to close input stage output", err)
		}
	}()
	jobLog.Info("Finished providing input. Running...")
	return &RunningJob{
		master: s.master,
		Job:    job,
	}, nil
}

func (s *session) createTask(worker *node.Node, req *lrmrpb.CreateTaskRequest) (string, error) {
	conn, err := s.master.nodeManager.Connect(context.TODO(), worker)
	if err != nil {
		return "", fmt.Errorf("grpc dial: %w", err)
	}
	w := lrmrpb.NewWorkerClient(conn)
	res, err := w.CreateTask(context.TODO(), req)
	if err != nil {
		return "", err
	}
	return res.TaskID, nil
}

// Output sets last stage output with given output spec.
func (s *session) Output(out *job.StageOutput) Session {
	s.stages[len(s.stages)-1].Output = out
	return s
}
