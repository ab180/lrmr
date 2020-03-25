package worker

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/airbloc/logger/module/loggergrpc"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	jsoniter "github.com/json-iterator/go"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"net"
	"path"
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

	opt *Options
}

func New(crd coordinator.Coordinator, opt *Options) (*Worker, error) {
	nm, err := node.NewManager(crd, node.DefaultManagerOptions())
	if err != nil {
		return nil, err
	}
	srv := grpc.NewServer(
		grpc.MaxRecvMsgSize(opt.MaxRecvSize),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			loggergrpc.UnaryServerLogger(log),
			loggergrpc.UnaryServerRecover(),
		)),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
				err := handler(srv, ss)
				if err != nil {
					ss.Context()
					md, ok := metadata.FromIncomingContext(ss.Context())
					if !ok {
						return err
					}
					entries := md.Get("dataHeader")
					if len(entries) < 1 {
						return err
					}
					header := new(lrmrpb.DataHeader)
					if e := jsoniter.UnmarshalFromString(entries[0], header); e != nil {
						return err
					}
					log.Error(" By {} (From {})", header.TaskID, header.FromHost)
				}
				return err
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

	w.jobReporter.Start()

	if err := w.nodeManager.RegisterSelf(ctx, node.New(w.opt.Host)); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}

	lrmrpb.RegisterWorkerServer(w.server, w)
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", w.opt.Bind, w.opt.Port))
	if err != nil {
		return err
	}
	return w.server.Serve(lis)
}

func (w *Worker) CreateTask(ctx context.Context, req *lrmrpb.CreateTaskRequest) (*empty.Empty, error) {
	j, err := w.jobManager.GetJob(ctx, req.Stage.JobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get job info: %v", err)
	}
	s := j.GetStage(req.Stage.Name)
	if s == nil {
		return nil, status.Errorf(codes.InvalidArgument, "stage %s not found on job %s", req.Stage.Name, req.Stage.JobID)
	}
	st := stage.Lookup(req.Stage.RunnerName)

	broadcasts := make(map[string]interface{})
	for key, broadcast := range req.Broadcasts {
		var val interface{}
		if err := msgpack.Decode(broadcast, &val); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "unable to unmarshal broadcast %s: %v", key, err)
		}
		broadcasts[key] = val
	}
	in := input.NewReader(w.opt.QueueLength)
	out, err := w.newOutputWriter(ctx, req.Stage, req.Output)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unable to create output: %v", err)
	}

	task := job.NewTask(req.PartitionKey, w.nodeManager.Self(), j, s)
	ts, err := w.jobManager.CreateTask(ctx, task)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create task failed: %v", err)
	}
	w.jobReporter.Add(task.Reference(), ts)

	log.Info("Create {}/{}/{} (Job ID: {})", j.Name, s.Name, task.PartitionKey, j.ID)
	log.Info("  Output: Partitioner {} with {} partitions", req.Output.Partitioner.String(), len(req.Output.PartitionToHost))

	c := NewTaskContext(w, task, broadcasts)
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
	return &empty.Empty{}, nil
}

func (w *Worker) newOutputWriter(ctx context.Context, s *lrmrpb.Stage, o *lrmrpb.Output) (*output.Writer, error) {
	var p output.Partitioner
	switch o.Partitioner {
	case lrmrpb.Output_FINITE_KEY:
		p = output.NewFiniteKeyPartitioner()
	case lrmrpb.Output_HASH_KEY:
		p = output.NewHashKeyPartitioner(len(o.PartitionToHost))
	default:
		p = output.NewShuffledPartitioner(len(o.PartitionToHost))
	}

	outputs := make(map[string]output.Output)
	for key, host := range o.PartitionToHost {
		taskID := path.Join(s.JobID, o.StageName, key)
		out, err := output.NewPushStream(ctx, w.nodeManager, host, taskID)
		if err != nil {
			return nil, err
		}
		outputs[key] = output.NewBufferedOutput(out, w.opt.Output.BufferLength)
	}
	return output.NewWriter(p, outputs), nil
}

func (w *Worker) PushData(stream lrmrpb.Worker_PushDataServer) error {
	exec, err := w.loadTaskExecutorFromHeader(stream)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	in := input.NewPushStream(exec.Input, stream)
	if err := in.Dispatch(); err != nil {
		return err
	}
	exec.WaitForFinish()
	return stream.SendAndClose(&empty.Empty{})
}

func (w *Worker) PollData(stream lrmrpb.Worker_PollDataServer) error {
	_, err := w.loadTaskExecutorFromHeader(stream)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	panic("implement me")
}

func (w *Worker) loadTaskExecutorFromHeader(stream grpc.ServerStream) (*TaskExecutor, error) {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return nil, errors.New("no metadata")
	}
	entries := md.Get("dataHeader")
	if len(entries) < 1 {
		return nil, errors.New("error parsing metadata: dataHeader is required")
	}
	header := new(lrmrpb.DataHeader)
	if err := jsoniter.UnmarshalFromString(entries[0], header); err != nil {
		return nil, errors.Wrap(err, "parse dataHeader")
	}
	r, ok := w.runningTasks.Load(header.TaskID)
	if !ok {
		return nil, errors.Errorf("task not found: %s", header.TaskID)
	}
	return r.(*TaskExecutor), nil
}

func (w *Worker) Stop() error {
	w.server.Stop()
	w.jobReporter.Close()
	return w.nodeManager.UnregisterNode(w.nodeManager.Self().ID)
}
