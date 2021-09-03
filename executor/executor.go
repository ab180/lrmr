package executor

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
	"github.com/ab180/lrmr/internal/errgroup"
	"github.com/ab180/lrmr/internal/serialization"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/job/stage"
	"github.com/ab180/lrmr/lrmrpb"
	"github.com/ab180/lrmr/metric"
	"github.com/ab180/lrmr/output"
	"github.com/ab180/lrmr/partitions"
	"github.com/airbloc/logger"
	"github.com/airbloc/logger/module/loggergrpc"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var log = logger.New("lrmr")

type Executor struct {
	Cluster   cluster.Cluster
	Node      node.Registration
	RPCServer *grpc.Server

	serverLis       net.Listener
	runningTasks    sync.Map
	workerLocalOpts map[string]interface{}
	cpuScheduler    CPUAffinityScheduler
	opt             Options
}

func New(crd coordinator.Coordinator, opt Options) (*Executor, error) {
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
	w := &Executor{
		Cluster:         c,
		RPCServer:       srv,
		workerLocalOpts: make(map[string]interface{}),
		cpuScheduler:    NewCPUAffinityScheduler(),
		opt:             opt,
	}
	if err := w.register(); err != nil {
		return nil, errors.WithMessage(err, "register executor")
	}
	return w, nil
}

func (w *Executor) register() error {
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

func (w *Executor) Start() error {
	return w.RPCServer.Serve(w.serverLis)
}

func (w *Executor) NumRunningTasks() (count int) {
	w.runningTasks.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return
}

func (w *Executor) SetWorkerLocalOption(key string, value interface{}) {
	w.workerLocalOpts[key] = value
}

func (w *Executor) State() node.State {
	return w.Node.States()
}

func (w *Executor) RunJobInForeground(req *lrmrpb.RunJobRequest, stream lrmrpb.Node_RunJobInForegroundServer) error {
	j := new(job.Job)
	if err := req.Job.UnmarshalJSON(j); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid JSON in Job: %v", err)
	}

	// replace last partition with collect
	j.Stages[len(j.Stages)-1].Output = stage.Output{
		Stage:       collectStageName,
		Partitioner: partitions.WrapPartitioner(NewCollector(stream)),
	}

	runningJob := newRunningJobHolder(j, newForegroundJobStatusReporter(stream))
	if err := w.createTasks(req, runningJob); err != nil {
		return err
	}
	taskReadySignal := &lrmrpb.RunOnlineJobOutputToDriver{Type: lrmrpb.RunOnlineJobOutputToDriver_TASKS_READY}
	if err := stream.Send(taskReadySignal); err != nil {
		return errors.Wrap(err, "send TASKS_READY signal to driver")
	}
	<-runningJob.Context().Done()
	return nil
}

func (w *Executor) RunJobInBackground(ctx context.Context, req *lrmrpb.RunJobRequest) (*empty.Empty, error) {
	j := new(job.Job)
	if err := req.Job.UnmarshalJSON(j); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid JSON in Job: %v", err)
	}
	runningJob := newRunningJobHolder(j, newBackgroundJobStatusReporter(w.Cluster.States(), j))
	if err := w.createTasks(req, runningJob); err != nil {
		return nil, err
	}
	return &empty.Empty{}, nil
}

func (w *Executor) GetMetric(ctx context.Context, req *lrmrpb.GetMetricRequest) (*lrmrpb.GetMetricResponse, error) {
	panic("not implemented")
}

func (w *Executor) createTasks(req *lrmrpb.RunJobRequest, runningJob *runningJobHolder) error {
	broadcasts, err := serialization.DeserializeBroadcast(req.Broadcasts)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	var wg errgroup.Group
	for _, stage := range req.Stages {
		for _, task := range stage.Tasks {
			stage, task := stage, task

			wg.Go(func() error {
				if err := w.createTask(runningJob, stage, task, broadcasts); err != nil {
					return errors.Wrapf(err, "create task %s/%s", stage.Name, task.PartitionID)
				}
				return nil
			})
		}
	}
	return wg.Wait()
}

func (w *Executor) createTask(runningJob *runningJobHolder, stageDesc *lrmrpb.Stage, taskDesc *lrmrpb.Task, broadcasts serialization.Broadcast) error {
	curStage := runningJob.Job.GetStage(stageDesc.Name)
	prevStage := runningJob.Job.GetPrevStageOf(curStage.Name)
	prevStageTasks := runningJob.Job.GetPartitionsOfStage(prevStage.Name)

	task := job.NewTask(taskDesc.PartitionID, w.Node.Info(), runningJob.Job.ID, curStage)

	in := input.NewReader(w.opt.Input.QueueLength, len(prevStageTasks))

	// after job finishes, remaining connections should be closed
	out, err := w.newOutputWriter(runningJob.Context(), runningJob.Job, stageDesc.Name, taskDesc.PartitionID, stageDesc.Output)
	if err != nil {
		return status.Errorf(codes.Internal, "unable to create output: %v", err)
	}

	exec := NewTaskExecutor(
		runningJob,
		task,
		curStage.Function,
		in,
		out,
		broadcasts,
		w.workerLocalOpts,
	)
	w.runningTasks.Store(task.ID().String(), exec)
	runningTasksGauge := lrmrmetric.RunningTasksGauge.With(lrmrmetric.WorkerLabelValuesFrom(w.Node.Info()))
	runningTasksGauge.Inc()

	go func() {
		if w.opt.ExperimentalCPUAffinity {
			occ := w.cpuScheduler.Occupy(task.ID().String())
			defer w.cpuScheduler.Release(occ)
		}
		exec.Run()

		w.runningTasks.Delete(task.ID().String())
		runningTasksGauge.Dec()
	}()
	return nil
}

func (w *Executor) newOutputWriter(ctx context.Context, j *job.Job, stageName, curPartitionID string, o *lrmrpb.Output) (*output.Writer, error) {
	idToOutput := make(map[string]output.Output)
	cur := j.GetStage(stageName)
	if cur.Output.Stage == "" {
		// last stage
		return output.NewWriter(curPartitionID, partitions.NewPreservePartitioner(), idToOutput), nil
	}
	if cur.Output.Stage == collectStageName {
		// use Collector as both output / partitioner
		collector := partitions.UnwrapPartitioner(cur.Output.Partitioner)
		idToOutput[collectPartitionID] = output.NewBufferedOutput(
			collector.(output.Output),
			w.opt.Output.BufferLength,
		)
		return output.NewWriter(curPartitionID, collector, idToOutput), nil
	}

	// only connect local
	if partitions.IsPreserved(cur.Output.Partitioner) {
		taskID := path.Join(j.ID, cur.Output.Stage, curPartitionID)

		idToOutput[curPartitionID] = output.LazyInitialized(func() (output.Output, error) {
			log.Verbose("Opening local input from {}/{} -> {}", cur.Name, curPartitionID, taskID)
			nextTask := w.getRunningTask(taskID)
			return NewLocalPipe(nextTask.Input), nil
		})
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
				mu.Lock()
				idToOutput[id] = NewLocalPipe(nextTask.Input)
				mu.Unlock()
				continue
			}
		}
		wg.Go(func() error {
			conn, err := w.Cluster.Connect(ctx, host)
			if err != nil {
				return errors.Wrapf(err, "dial %s", host)
			}
			out := output.LazyInitialized(func() (output.Output, error) {
				return output.OpenPushStream(ctx, lrmrpb.NewNodeClient(conn), w.Node.Info(), host, taskID)
			})
			mu.Lock()
			defer mu.Unlock()
			idToOutput[id] = output.NewBufferedOutput(out, w.opt.Output.BufferLength)
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	return output.NewWriter(curPartitionID, partitions.UnwrapPartitioner(cur.Output.Partitioner), idToOutput), nil
}

func (w *Executor) getRunningTask(taskID string) *TaskExecutor {
	task, ok := w.runningTasks.Load(taskID)
	if !ok {
		return nil
	}
	return task.(*TaskExecutor)
}

func (w *Executor) PushData(stream lrmrpb.Node_PushDataServer) error {
	h, err := lrmrpb.DataHeaderFromMetadata(stream)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	exec := w.getRunningTask(h.TaskID)
	if exec == nil {
		return status.Errorf(codes.InvalidArgument, "task %s not found on %s", h.TaskID, w.Node.Info().Host)
	}

	log.Verbose("PushData({})", h.TaskID)
	in := input.NewPushStream(exec.Input, stream)
	if err := in.Dispatch(); err != nil {
		return err
	}

	// upstream may have been closed, but that should not affect the task result
	_ = stream.SendAndClose(&empty.Empty{})
	return nil
}

func (w *Executor) PollData(stream lrmrpb.Node_PollDataServer) error {
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

func (w *Executor) Close() error {
	w.RPCServer.GracefulStop()
	w.Node.Unregister()
	return w.Cluster.Close()
}

func errorLogMiddleware(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	// dump header on stream failure
	if err := handler(srv, ss); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		if h, herr := lrmrpb.DataHeaderFromMetadata(ss); herr == nil {
			log.Error("{} called by {} failed: {}", h.TaskID, h.FromHost, err)
		}
		return err
	}
	return nil
}
