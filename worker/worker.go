package worker

import (
	"context"
	"fmt"
	"net"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/airbloc/logger"
	"github.com/airbloc/logger/module/loggergrpc"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/input"
	"github.com/therne/lrmr/internal/serialization"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/partitions"
	"github.com/therne/lrmr/stage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var log = logger.New("lrmr")

type Worker struct {
	ClusterState coordinator.Coordinator
	NodeManager  node.Manager
	jobManager   *job.Manager
	jobReporter  *job.Reporter
	RPCServer    *grpc.Server

	runningTasksMu  sync.RWMutex
	runningTasks    map[string]*TaskExecutor
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
		grpc.UnaryInterceptor(loggergrpc.UnaryServerRecover()),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			errorLogMiddleware,
			loggergrpc.StreamServerRecover(),
		)),
	)
	return &Worker{
		ClusterState:    crd,
		NodeManager:     nm,
		jobReporter:     job.NewJobReporter(crd),
		jobManager:      job.NewManager(nm, crd),
		RPCServer:       srv,
		runningTasks:    make(map[string]*TaskExecutor),
		workerLocalOpts: make(map[string]interface{}),
		opt:             opt,
	}, nil
}

func (w *Worker) SetWorkerLocalOption(key string, value interface{}) {
	w.workerLocalOpts[key] = value
}

func (w *Worker) State() coordinator.KV {
	return w.NodeManager.NodeStates()
}

func (w *Worker) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lrmrpb.RegisterNodeServer(w.RPCServer, w)
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
	n.Tag = w.opt.NodeTags
	n.Executors = w.opt.Concurrency
	if err := w.NodeManager.RegisterSelf(ctx, n); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	w.jobReporter.Start()
	return w.RPCServer.Serve(lis)
}

func (w *Worker) CreateTasks(ctx context.Context, req *lrmrpb.CreateTasksRequest) (*empty.Empty, error) {
	broadcasts, err := serialization.DeserializeBroadcast(req.Broadcasts)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	wg, wctx := errgroup.WithContext(ctx)
	for _, p := range req.PartitionIDs {
		partitionID := p
		wg.Go(func() error { return w.createTask(wctx, req, partitionID, broadcasts) })
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	// log.Info("Create {}/{}/[{}] (Output {})", req.Job.Name, s.Name, strings.Join(req.PartitionIDs, ","))
	return &empty.Empty{}, nil
}

func (w *Worker) createTask(ctx context.Context, req *lrmrpb.CreateTasksRequest, partitionID string, broadcasts serialization.Broadcast) error {
	var s stage.Stage
	if err := req.Stage.UnmarshalJSON(&s); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid stage JSON: %v", err)
	}

	task := job.NewTask(partitionID, w.NodeManager.Self(), req.Job.Id, s)
	ts, err := w.jobManager.CreateTask(ctx, task)
	if err != nil {
		return status.Errorf(codes.Internal, "create task failed: %v", err)
	}
	w.jobReporter.Add(task.Reference(), ts)

	taskCtx := newTaskContext(context.Background(), w, req.Job.Id, task, broadcasts)
	in := input.NewReader(w.opt.Input.QueueLength)
	out, err := w.newOutputWriter(taskCtx, req.Job.Id, s, req.Output)
	if err != nil {
		return status.Errorf(codes.Internal, "unable to create output: %v", err)
	}

	exec, err := NewTaskExecutor(taskCtx, task, s.Function, in, out)
	if err != nil {
		err = errors.Wrap(err, "failed to start executor")
		if reportErr := w.jobReporter.ReportFailure(task.Reference(), err); reportErr != nil {
			return reportErr
		}
		return err
	}
	w.runningTasksMu.Lock()
	w.runningTasks[task.Reference().String()] = exec
	w.runningTasksMu.Unlock()

	go exec.Run()
	return nil
}

func (w *Worker) newOutputWriter(ctx *taskContext, jobID string, cur stage.Stage, o *lrmrpb.Output) (output.Output, error) {
	idToOutput := make(map[string]output.Output)
	for id, host := range o.PartitionToHost {
		taskID := path.Join(jobID, cur.Output.Stage, id)
		if host == w.NodeManager.Self().Host {
			t := w.getRunningTask(taskID)
			if t != nil {
				idToOutput[id] = NewLocalPipe(t.Input)
				continue
			}
		}
		out, err := output.NewPushStream(ctx, w.NodeManager, host, taskID)
		if err != nil {
			return nil, err
		}
		idToOutput[id] = output.NewBufferedOutput(out, w.opt.Output.BufferLength)
	}
	return output.NewWriter(ctx, partitions.UnwrapPartitioner(cur.Output.Partitioner), idToOutput), nil
}

func (w *Worker) getRunningTask(taskID string) *TaskExecutor {
	w.runningTasksMu.RLock()
	defer w.runningTasksMu.RUnlock()
	return w.runningTasks[taskID]
}

func (w *Worker) PushData(stream lrmrpb.Node_PushDataServer) error {
	h, err := lrmrpb.DataHeaderFromMetadata(stream)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	exec := w.getRunningTask(h.TaskID)
	if exec == nil {
		return status.Errorf(codes.InvalidArgument, "task not found: %s", h.TaskID)
	}
	defer func() {
		w.runningTasksMu.Lock()
		delete(w.runningTasks, h.TaskID)
		w.runningTasksMu.Unlock()
	}()

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
	exec := w.getRunningTask(h.TaskID)
	if exec == nil {
		return status.Errorf(codes.InvalidArgument, "task not found: %s", h.TaskID)
	}
	panic("implement me")
}

func (w *Worker) Stop() error {
	w.RPCServer.Stop()
	w.jobReporter.Close()
	if err := w.NodeManager.UnregisterSelf(); err != nil {
		return errors.Wrap(err, "unregister node")
	}
	return w.NodeManager.Close()
}

func errorLogMiddleware(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// dump header on stream failure
	if err := handler(srv, ss); err != nil {
		if errors.Cause(err) == context.Canceled {
			return nil
		}
		if h, herr := lrmrpb.DataHeaderFromMetadata(ss); herr == nil {
			log.Error("{} failed: {}\nBy {} (From {})", info.FullMethod, err, h.TaskID, h.FromHost)
		}
		return err
	}
	return nil
}
