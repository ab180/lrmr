package worker

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/airbloc/logger/module/loggergrpc"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/pkg/errors"
	"github.com/shamaton/msgpack"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/input"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"path"
	"strings"
	"sync"
	"time"
)

var log = logger.New("worker")

type Worker struct {
	nodeManager node.Manager
	jobManager  job.Manager
	jobReporter *job.Reporter
	server      *grpc.Server

	runningTasks    sync.Map
	workerLocalOpts map[string]interface{}

	opt Options
}

func New(crd coordinator.Coordinator, opt Options) (*Worker, error) {
	nm, err := node.NewManager(crd, node.DefaultManagerOptions())
	if err != nil {
		return nil, err
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(opt.Input.MaxRecvSize),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			loggergrpc.UnaryServerLogger(log),
			loggergrpc.UnaryServerRecover(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				// dump header on stream failure
				if err := handler(srv, ss); err != nil {
					if h, err := lrmrpb.DataHeaderFromMetadata(ss); err == nil {
						log.Error(" By {} (From {})", h.TaskID, h.FromHost)
					}
					return err
				}
				return nil
			},
			loggergrpc.StreamServerLogger(log),
			loggergrpc.StreamServerRecover(),
		)),
	)
	return &Worker{
		nodeManager:     nm,
		jobReporter:     job.NewJobReporter(crd),
		jobManager:      job.NewManager(nm, crd),
		server:          srv,
		workerLocalOpts: make(map[string]interface{}),
		opt:             opt,
	}, nil
}

func (w *Worker) getWorkerLocalOption(key string) interface{} {
	return w.workerLocalOpts[key]
}

func (w *Worker) SetWorkerLocalOption(key string, value interface{}) {
	w.workerLocalOpts[key] = value
}

func (w *Worker) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lrmrpb.RegisterNodeServer(w.server, w)
	lis, err := net.Listen("tcp", w.opt.ListenHost)
	if err != nil {
		return err
	}
	advHost := w.opt.AdvertisedHost
	if strings.HasSuffix(advHost, ":") {
		// port is assigned automatically
		addrFrags := strings.Split(lis.Addr().String(), ":")
		advHost += addrFrags[len(addrFrags)-1]
	}

	n := node.New(advHost, node.Worker)
	if err := w.nodeManager.RegisterSelf(ctx, n); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	w.jobReporter.Start()
	return w.server.Serve(lis)
}

func (w *Worker) CreateTask(ctx context.Context, req *lrmrpb.CreateTaskRequest) (*empty.Empty, error) {
	j, err := w.jobManager.GetJob(ctx, req.JobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get job info: %v", err)
	}
	s := j.GetStage(req.StageName)
	if s == nil {
		return nil, status.Errorf(codes.InvalidArgument, "stage %s not found on job %s", req.StageName, j.ID)
	}
	st := stage.Lookup(s.RunnerName)

	broadcasts := make(stage.Broadcasts)
	for key, data := range req.Broadcasts {
		var val interface{}
		if err := msgpack.Decode(data, &val); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "unable to unmarshal broadcast %s: %v", key, err)
		}
		broadcasts[key] = val
	}
	in := input.NewReader(w.opt.Input.QueueLength)
	out, err := w.newOutputWriter(ctx, j, s, req.Output, req.Input.PartitionKey)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unable to create output: %v", err)
	}

	task := job.NewTask(req.Input.PartitionKey, w.nodeManager.Self(), j, s)
	ts, err := w.jobManager.CreateTask(ctx, task)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create task failed: %v", err)
	}
	w.jobReporter.Add(task.Reference(), ts)

	log.Info("Create {}/{}/{} (Job ID: {})", j.Name, s.Name, task.PartitionKey, j.ID)
	log.Info("  Output: Partitioner {} with {} partitions", req.Output.Partitioner.String(), out.NumOutputs())

	c := newTaskContext(w, task, broadcasts)
	exec, err := NewTaskExecutor(c, task, st, in, out)
	if err != nil {
		err = errors.Wrap(err, "failed to start executor")
		if reportErr := w.jobReporter.ReportFailure(task.Reference(), err); reportErr != nil {
			return nil, reportErr
		}
		return nil, err
	}
	taskID := path.Join(j.ID, s.Name, task.PartitionKey)
	w.runningTasks.Store(taskID, exec)
	go exec.Run()
	go func() {
		for reason := range w.jobManager.WatchJobErrors(exec.context, exec.task.JobID) {
			log.Warn("Task {} canceled because job is aborted. Reason: {}", exec.task.Reference(), reason)
			exec.Cancel()
			break
		}
	}()
	return &empty.Empty{}, nil
}

func (w *Worker) newOutputWriter(ctx context.Context, j *job.Job, s *job.Stage, o *lrmrpb.Output, partitionKey string) (*output.Writer, error) {
	var p output.Partitioner
	switch o.Partitioner {
	case lrmrpb.Output_FINITE_KEY:
		p = output.NewFiniteKeyPartitioner(s.Partitions.Keys())
	case lrmrpb.Output_HASH_KEY:
		p = output.NewHashKeyPartitioner(len(o.PartitionToHost))
	case lrmrpb.Output_SHUFFLE:
		p = output.NewShuffledPartitioner(len(o.PartitionToHost))

	case lrmrpb.Output_PRESERVE:
		// PRESERVE mode does not use network since input and output partitions are same
		nextTaskID := path.Join(j.ID, o.NextStageName, partitionKey)
		t, ok := w.runningTasks.Load(nextTaskID)
		if !ok {
			return nil, errors.Errorf("output mode was PRESERVE, but corresponding task %s is not found", nextTaskID)
		}
		outputs := map[string]output.Output{
			partitionKey: NewLocalPipe(t.(*TaskExecutor).Input),
		}
		p = output.NewPreservePartitioner(partitionKey)
		return output.NewWriter(p, outputs), nil
	}

	outputs := make(map[string]output.Output)
	for key, host := range o.PartitionToHost {
		taskID := path.Join(j.ID, o.NextStageName, key)
		if host == w.nodeManager.Self().Host {
			t, ok := w.runningTasks.Load(taskID)
			if ok {
				outputs[key] = NewLocalPipe(t.(*TaskExecutor).Input)
				continue
			}
		}
		out, err := output.NewPushStream(ctx, w.nodeManager, host, taskID)
		if err != nil {
			return nil, err
		}
		outputs[key] = output.NewBufferedOutput(out, w.opt.Output.BufferLength)
	}
	return output.NewWriter(p, outputs), nil
}

func (w *Worker) PushData(stream lrmrpb.Node_PushDataServer) error {
	h, err := lrmrpb.DataHeaderFromMetadata(stream)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	e, ok := w.runningTasks.Load(h.TaskID)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "task not found: %s", h.TaskID)
	}
	exec := e.(*TaskExecutor)
	defer w.runningTasks.Delete(h.TaskID)

	in := input.NewPushStream(exec.Input, stream)
	if err := in.Dispatch(exec.context); err != nil {
		return err
	}
	exec.WaitForFinish()
	return stream.SendAndClose(&empty.Empty{})
}

func (w *Worker) PollData(stream lrmrpb.Node_PollDataServer) error {
	h, err := lrmrpb.DataHeaderFromMetadata(stream)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	e, ok := w.runningTasks.Load(h.TaskID)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "task not found: %s", h.TaskID)
	}
	_ = e.(*TaskExecutor)
	panic("implement me")
}

func (w *Worker) Stop() error {
	w.server.Stop()
	w.jobReporter.Close()
	if err := w.nodeManager.UnregisterNode(w.nodeManager.Self().ID); err != nil {
		return errors.Wrap(err, "unregister node")
	}
	return w.nodeManager.Close()
}
