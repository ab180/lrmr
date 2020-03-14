package worker

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/airbloc/logger/module/loggergrpc"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/ivpusic/grpool"
	"github.com/shamaton/msgpack"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"net"
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

func (w *Worker) NodeInfo() *lrmrpb.Node {
	n := w.nodeManager.Self()
	return &lrmrpb.Node{
		Host: n.Host,
		ID:   n.ID,
	}
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

func (w *Worker) CreateTask(ctx context.Context, req *lrmrpb.CreateTaskRequest) (*lrmrpb.CreateTaskResponse, error) {
	j, err := w.jobManager.GetJob(ctx, req.Stage.JobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get job info: %w", err)
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
			return nil, status.Errorf(codes.InvalidArgument, "unable to unmarshal broadcast %s: %w", key, err)
		}
		broadcasts[key] = val
	}

	shards, err := output.DialShards(ctx, w, req.Output, w.opt.Output)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unable to connect output: %w", err)
	}

	task := job.NewTask(w.nodeManager.Self(), j, s)
	ts, err := w.jobManager.CreateTask(ctx, task)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create task failed: %w", err)
	}
	w.jobReporter.Add(task.Reference(), ts)

	log.Info("Create task {} of {}/{} (Job: {})", task.ID, j.ID, s.Name, j.Name)
	log.Info("  Output: Partition({}), Shards({})", req.Output.Partitioner.Type.String(), len(req.Output.Shards))

	t := &runningTask{
		worker:     w,
		job:        j,
		stage:      s,
		task:       task,
		shards:     shards,
		pool:       grpool.NewPool(w.opt.PoolSize, w.opt.QueueLength),
		broadcasts: broadcasts,
	}
	t.executors = newExecutors(st, t, shards)
	w.runningTasks.Store(task.ID, t)
	return &lrmrpb.CreateTaskResponse{TaskID: task.ID}, nil
}

func (w *Worker) RunTask(stream lrmrpb.Worker_RunTaskServer) error {
	var t *runningTask
	defer t.TryRecover()

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return t.Abort(fmt.Errorf("stream recv: %v", err))
		}
		if t == nil {
			// first event of the stream. warm up
			rt, ok := w.runningTasks.Load(req.TaskID)
			if !ok {
				return status.Errorf(codes.NotFound, "task not found: %s", req.TaskID)
			}
			t = rt.(*runningTask)
			t.addConnection(stream)

			var from string
			if _, ok := stream.(*output.LocalPipeStream); ok {
				from = "local"
			} else {
				from = fmt.Sprintf("%s (%s)", req.From.Host, req.From.ID)
			}
			log.Debug("Task {} ({}/{}) connected with {}", req.TaskID, t.job.ID, t.stage.Name, from)

		} else if !t.isRunning {
			t.isRunning = true
			w.jobReporter.UpdateStatus(t.task.Reference(), func(ts *job.TaskStatus) {
				ts.Status = job.Running
			})
		}
		for _, data := range req.Inputs {
			if err := t.executors.Apply(data); err != nil {
				return t.Abort(err)
			}
		}
	}
	if t == nil {
		return status.Errorf(codes.InvalidArgument, "no events but found EOF")
	}

	if allDone := t.finishConnection(); allDone {
		w.runningTasks.Delete(t.task.ID)
		return t.Finish()
	}
	return stream.SendAndClose(&empty.Empty{})
}

func (w *Worker) Stop() error {
	w.server.Stop()
	w.jobReporter.Close()
	return w.nodeManager.UnregisterNode(w.nodeManager.Self().ID)
}
