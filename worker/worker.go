package worker

import (
	"context"
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
	"github.com/therne/lrmr/cluster/node"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/input"
	"github.com/therne/lrmr/internal/serialization"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/partitions"
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

	RPCServer *grpc.Server
	serverLis net.Listener

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
	w := &Worker{
		ClusterState:    crd,
		NodeManager:     nm,
		jobManager:      job.NewManager(crd),
		RPCServer:       srv,
		runningTasks:    make(map[string]*TaskExecutor),
		workerLocalOpts: make(map[string]interface{}),
		opt:             opt,
	}
	if err := w.register(); err != nil {
		return nil, errors.WithMessage(err, "register worker")
	}
	return w, nil
}

func (w *Worker) register() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lrmrpb.RegisterNodeServer(w.RPCServer, w)

	// if port is not specified on ListenHost, it must be automatically
	// assigned with any available port in system by net.Listen.
	lis, err := net.Listen("tcp", w.opt.ListenHost)
	if err != nil {
		return errors.Wrapf(err, "listen %s", w.opt.ListenHost)
	}
	w.serverLis = lis

	advHost := w.opt.AdvertisedHost
	if strings.HasSuffix(advHost, ":") {
		// port is assigned automatically
		_, actualPort, _ := net.SplitHostPort(lis.Addr().String())
		advHost += actualPort
	}
	n := node.New(advHost, w.opt.NodeType)
	n.Tag = w.opt.NodeTags
	n.Executors = w.opt.Concurrency

	return w.NodeManager.RegisterSelf(ctx, n)
}

func (w *Worker) Start() error {
	return w.RPCServer.Serve(w.serverLis)
}

func (w *Worker) SetWorkerLocalOption(key string, value interface{}) {
	w.workerLocalOpts[key] = value
}

func (w *Worker) State() coordinator.KV {
	return w.NodeManager.NodeStates()
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
	return &empty.Empty{}, nil
}

func (w *Worker) createTask(ctx context.Context, req *lrmrpb.CreateTasksRequest, partitionID string, broadcasts serialization.Broadcast) error {
	j := new(job.Job)
	if err := req.Job.UnmarshalJSON(j); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid JSON in Job: %v", err)
	}
	s := j.GetStage(req.Stage)

	task := job.NewTask(partitionID, w.NodeManager.Self(), j.ID, s)
	ts, err := w.jobManager.CreateTask(ctx, task)
	if err != nil {
		return status.Errorf(codes.Internal, "create task failed: %v", err)
	}

	in := input.NewReader(w.opt.Input.QueueLength)
	out, err := w.newOutputWriter(ctx, j, s.Name, partitionID, req.Output)
	if err != nil {
		return status.Errorf(codes.Internal, "unable to create output: %v", err)
	}

	exec := NewTaskExecutor(w.ClusterState, j, task, ts, s.Function, in, out, broadcasts, w.workerLocalOpts)
	w.runningTasksMu.Lock()
	w.runningTasks[task.ID().String()] = exec
	w.runningTasksMu.Unlock()

	go exec.Run()
	return nil
}

func (w *Worker) newOutputWriter(ctx context.Context, j *job.Job, stageName, curPartitionID string, o *lrmrpb.Output) (output.Output, error) {
	idToOutput := make(map[string]output.Output)
	cur := j.GetStage(stageName)
	if cur.Output.Stage == "" {
		// last stage
		return output.NewWriter(curPartitionID, partitions.NewPreservePartitioner(), idToOutput), nil
	}

	// only connect local
	if partitions.IsPreserved(cur.Output.Partitioner) {
		taskID := path.Join(j.ID, cur.Output.Stage, curPartitionID)
		nextTask := w.getRunningTask(taskID)

		idToOutput[curPartitionID] = NewLocalPipe(nextTask.Input)
		return output.NewWriter(curPartitionID, partitions.NewPreservePartitioner(), idToOutput), nil
	}

	var mu sync.Mutex
	wg, wctx := errgroup.WithContext(ctx)
	for i, h := range o.PartitionToHost {
		id, host := i, h

		taskID := path.Join(j.ID, cur.Output.Stage, id)
		if host == w.NodeManager.Self().Host {
			nextTask := w.getRunningTask(taskID)
			if nextTask != nil {
				idToOutput[id] = NewLocalPipe(nextTask.Input)
				continue
			}
		}
		wg.Go(func() error {
			out, err := output.NewPushStream(wctx, w.NodeManager, host, taskID)
			if err != nil {
				return err
			}
			mu.Lock()
			idToOutput[id] = output.NewBufferedOutput(out, w.opt.Output.BufferLength)
			mu.Unlock()
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	return output.NewWriter(curPartitionID, partitions.UnwrapPartitioner(cur.Output.Partitioner), idToOutput), nil
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

	// upstream may have been closed, but that should not affect the task result
	_ = stream.SendAndClose(&empty.Empty{})
	return nil
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
	// for {
	// 	req, err := stream.Recv()
	// 	if err != nil {
	// 		if err == io.EOF {
	// 			break
	// 		}
	// 		return
	// 	}
	// }
	panic("implement me")
}

func (w *Worker) Close() error {
	w.RPCServer.Stop()
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
			log.Error("{} called by {} failed: {}", h.TaskID, h.FromHost, err)
		}
		return err
	}
	return nil
}
