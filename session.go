package lrmr

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/shamaton/msgpack"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
)

type Result struct {
	Ok     bool
	Errors []error
	Output interface{}
}

type Session interface {
	Broadcast(key string, val interface{})
	AddStage(name string, tf transformation.Transformation) Session
	Output(out *node.StageOutput) Session

	Run(ctx context.Context, name string) (*RunningJob, error)
}

type session struct {
	master *Master
	stages []*node.Stage
	tfs    []transformation.Transformation

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

func (s *session) AddStage(name string, tf transformation.Transformation) Session {
	defaultOut := node.DescribingStageOutput().WithFanout()
	s.stages = append(s.stages, node.NewStage(name, transformation.NameOf(tf), defaultOut))
	s.tfs = append(s.tfs, tf)

	data, err := msgpack.Encode(tf)
	if err != nil {
		panic(fmt.Sprintf("broadcasting %s: %v", name, err))
	}
	s.broadcasts["__stage/"+name] = data
	s.serializedBroadcasts["__stage/"+name] = data
	return s
}

func (s *session) Run(ctx context.Context, name string) (*RunningJob, error) {
	job, err := s.master.nodeManager.CreateJob(ctx, name, s.stages)
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

	var inputStageOutput *output.Shards

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
				JobID:          job.ID,
				Name:           stage.Name,
				Transformation: stage.Transformation,
			},
			Output:     stage.Output.Build(prevShards),
			Broadcasts: s.serializedBroadcasts,
		}

		if i == 0 {
			// prepare input stage (1st stage)
			if err := s.tfs[0].Setup(nil); err != nil {
				return nil, fmt.Errorf("input stage setup: %w", err)
			}
			inputStageOutput, err = output.DialShards(ctx, s.master.node, req.Output, s.master.opt.Master.Output)
			if err != nil {
				// plot twist ¯\_(ツ)_/¯
				return nil, fmt.Errorf("input stage output setup: %w", err)
			}
			break
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
	out := output.NewStreamWriter(inputStageOutput)
	if err := s.tfs[0].Apply(nil, nil, out); err != nil {
		return nil, fmt.Errorf("running input: %w", err)
	}
	if err := out.Flush(); err != nil {
		return nil, fmt.Errorf("flushing input: %w", err)
	}
	go func() {
		if err := inputStageOutput.Close(); err != nil {
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
func (s *session) Output(out *node.StageOutput) Session {
	s.stages[len(s.stages)-1].Output = out
	return s
}
