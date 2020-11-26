package worker

import (
	"context"
	"io"
	"net"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/input"
	"github.com/ab180/lrmr/internal/serialization"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrmrpb"
	"github.com/ab180/lrmr/output"
	"github.com/ab180/lrmr/partitions"
	"github.com/airbloc/logger"
	"github.com/airbloc/logger/module/loggergrpc"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var log = logger.New("lrmr")

type Worker struct {
	Cluster   cluster.Cluster
	Node      node.Registration
	RPCServer *grpc.Server

	serverLis       net.Listener
	jobManager      *job.Manager
	jobTracker      *job.Tracker
	runningTasks    sync.Map
	workerLocalOpts map[string]interface{}

	opt Options
}

func New(crd coordinator.Coordinator, opt Options) (*Worker, error) {
	c, err := cluster.OpenRemote(crd, cluster.DefaultOptions())
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
	jm := job.NewManager(c.States())
	w := &Worker{
		Cluster:         c,
		jobManager:      jm,
		jobTracker:      job.NewJobTracker(c.States(), jm),
		RPCServer:       srv,
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

	nr, err := w.Cluster.Register(ctx, n)
	if err != nil {
		return err
	}
	w.Node = nr
	return nil
}

func (w *Worker) Start() error {
	return w.RPCServer.Serve(w.serverLis)
}

func (w *Worker) SetWorkerLocalOption(key string, value interface{}) {
	w.workerLocalOpts[key] = value
}

func (w *Worker) State() node.State {
	return w.Node.States()
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

	// jobCtx will be disposed after the job completes
	jobCtx, cancelJobCtx := context.WithCancel(context.Background())

	task := job.NewTask(partitionID, w.Node.Info(), j.ID, s)
	ts, err := w.jobManager.CreateTask(ctx, task)
	if err != nil {
		return status.Errorf(codes.Internal, "create task failed: %v", err)
	}
	in := input.NewReader(w.opt.Input.QueueLength)

	// after job finishes, remaining connections should be closed
	out, err := w.newOutputWriter(jobCtx, j, s.Name, partitionID, req.Output)
	if err != nil {
		return status.Errorf(codes.Internal, "unable to create output: %v", err)
	}

	exec := NewTaskExecutor(jobCtx, w.Cluster.States(), j, task, ts, s.Function, in, out, broadcasts, w.workerLocalOpts)
	w.runningTasks.Store(task.ID().String(), exec)

	w.jobTracker.OnJobCompletion(j, func(j *job.Job, stat *job.Status) {
		if len(stat.Errors) > 0 {
			err := stat.Errors[0]
			log.Verbose("Task {} aborted with error caused by task {}.", task.ID(), err.Task)
			exec.Abort(nil)
		}
		cancelJobCtx()
	})
	go exec.Run()
	return nil
}

func (w *Worker) newOutputWriter(ctx context.Context, j *job.Job, stageName, curPartitionID string, o *lrmrpb.Output) (*output.Writer, error) {
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
	var wg errgroup.Group
	for i, h := range o.PartitionToHost {
		id, host := i, h

		taskID := path.Join(j.ID, cur.Output.Stage, id)
		if host == w.Node.Info().Host {
			nextTask := w.getRunningTask(taskID)
			if nextTask != nil {
				idToOutput[id] = NewLocalPipe(nextTask.Input)
				continue
			}
		}
		wg.Go(func() error {
			out, err := output.OpenPushStream(ctx, w.Cluster, w.Node.Info(), host, taskID)
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
	task, _ := w.runningTasks.Load(taskID)
	return task.(*TaskExecutor)
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
	defer w.runningTasks.Delete(h.TaskID)

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
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		rows, err := exec.Output.Dispatch(h.TaskID, int(req.N))
		if err != nil {
			return err
		}
		resp := &lrmrpb.PollDataResponse{Data: rows}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	panic("implement me")
}

func (w *Worker) Close() error {
	w.RPCServer.Stop()
	w.Node.Unregister()
	w.jobTracker.Close()
	return w.Cluster.Close()
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
