package executor

import (
	"context"
	"net"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/input"
	"github.com/ab180/lrmr/internal/cpuaffinity"
	"github.com/ab180/lrmr/internal/errgroup"
	"github.com/ab180/lrmr/internal/pbtypes"
	"github.com/ab180/lrmr/internal/serialization"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/lrmrpb"
	lrmrmetric "github.com/ab180/lrmr/metric"
	"github.com/ab180/lrmr/output"
	"github.com/ab180/lrmr/partitions"
	"github.com/airbloc/logger"
	"github.com/airbloc/logger/module/loggergrpc"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var log = logger.New("lrmr")

type Executor struct {
	lrmrpb.UnimplementedNodeServer

	Cluster   cluster.Cluster
	Node      node.Registration
	RPCServer *grpc.Server

	serverLis       net.Listener
	runningTasks    sync.Map
	runningJobs     sync.Map
	workerLocalOpts map[string]interface{}
	cpuScheduler    cpuaffinity.Scheduler
	opt             Options
}

func New(c cluster.Cluster, options ...func(*Executor)) (*Executor, error) {
	w := &Executor{
		Cluster:         c,
		workerLocalOpts: make(map[string]interface{}),
		cpuScheduler:    cpuaffinity.NewScheduler(),
		opt:             DefaultOptions(),
	}
	for _, o := range options {
		o(w)
	}

	w.RPCServer = grpc.NewServer(
		grpc.MaxRecvMsgSize(w.opt.MaxMessageSize),
		grpc.MaxSendMsgSize(w.opt.MaxMessageSize),
		grpc.UnaryInterceptor(loggergrpc.UnaryServerRecover()),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			errorLogMiddleware,
			loggergrpc.StreamServerRecover(),
		)),
	)

	if err := w.register(); err != nil {
		return nil, errors.WithMessage(err, "register executor")
	}
	return w, nil
}

func (w *Executor) register() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lrmrpb.RegisterNodeServer(w.RPCServer, w)

	if w.serverLis == nil {
		// if port is not specified on ListenHost, it must be automatically
		// assigned with any available port in system by net.Listen.
		lis, err := net.Listen("tcp", w.opt.ListenHost)
		if err != nil {
			return errors.Wrapf(err, "listen %s", w.opt.ListenHost)
		}
		w.serverLis = lis
	}

	advHost := w.opt.AdvertisedHost
	if strings.HasSuffix(advHost, ":") {
		// port is assigned automatically
		_, actualPort, _ := net.SplitHostPort(w.serverLis.Addr().String())
		advHost += actualPort
	}
	n := node.New(advHost)
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

// Concurrency returns desired number of the executor threads in an executor.
func (w *Executor) Concurrency() int {
	return w.opt.Concurrency
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

func (w *Executor) CreateJob(_ context.Context, req *lrmrpb.CreateJobRequest) (*pbtypes.Empty, error) {
	j := new(job.Job)
	if err := req.Job.Unmarshal(j); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid JSON in Job: %v", err)
	}
	broadcasts, err := serialization.DeserializeBroadcast(req.Broadcasts)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	runningJob := newRunningJobHolder(j, broadcasts)
	for _, s := range req.Stages {
		for _, t := range s.Tasks {
			curStage := runningJob.Job.GetStage(s.Name)

			task := job.NewTask(t.PartitionID, w.Node.Info(), runningJob.Job.ID, curStage)
			in := input.NewReader(w.opt.Input.QueueLength, lrdd.RowType(s.RowType))
			taskExec := NewTaskExecutor(runningJob, task, curStage, in, s.Output, w.workerLocalOpts, &w.opt)

			runningJob.Tasks = append(runningJob.Tasks, taskExec)
			w.runningTasks.Store(task.ID().String(), taskExec)
		}
	}
	w.runningJobs.Store(j.ID, runningJob)
	return &pbtypes.Empty{}, nil
}

func (w *Executor) StartJobInForeground(req *lrmrpb.StartJobRequest, stream lrmrpb.Node_StartJobInForegroundServer) error { //nolint:lll
	v, ok := w.runningJobs.Load(req.JobID)
	if !ok {
		return status.Error(codes.NotFound, "job not found")
	}
	runningJob := v.(*runningJobHolder)
	defer w.runningJobs.Delete(req.JobID)

	if err := w.startJob(runningJob, newAttachedStatusReporter(req.JobID, stream)); err != nil {
		return err
	}
	<-runningJob.Context().Done()
	return nil
}

func (w *Executor) StartJobInBackground(ctx context.Context, req *lrmrpb.StartJobRequest) (*pbtypes.Empty, error) {
	v, ok := w.runningJobs.Load(req.JobID)
	if !ok {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	runningJob := v.(*runningJobHolder)
	if err := w.startJob(runningJob, newDetachedStatusReporter(w.Cluster.States(), runningJob.Job)); err != nil {
		return nil, err
	}
	go func() {
		<-runningJob.Context().Done()
		w.runningJobs.Delete(req.JobID)
	}()
	return &pbtypes.Empty{}, nil
}

func (w *Executor) startJob(runningJob *runningJobHolder, reporter StatusReporter) error {
	runningJob.Start(reporter)

	var wg errgroup.Group
	for _, taskExec := range runningJob.Tasks {
		taskExec := taskExec
		wg.Go(func() error {
			out, err := w.openOutput(taskExec)
			if err != nil {
				return errors.Wrapf(err, "open output in %s", taskExec.task.ID())
			}
			taskExec.SetOutput(out)
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return err
	}
	for _, taskExec := range runningJob.Tasks {
		go w.startTask(taskExec)
	}
	return nil
}

func (w *Executor) openOutput(taskExec *TaskExecutor) (*output.Writer, error) {
	var (
		runningJob = taskExec.job
		curStage   = taskExec.Stage
		task       = taskExec.task
		idToOutput = make(map[string]output.Output)
	)
	if curStage.Output.Stage == "" {
		// last stage
		return output.NewWriter(task.PartitionID, partitions.NewPreservePartitioner(), idToOutput), nil
	}
	if curStage.Output.Stage == CollectStageName {
		// last stage (with collect)
		idToOutput[collectPartitionID] = output.NewBufferedOutput(
			NewCollector(runningJob.Reporter),
			w.opt.Output.BufferLength,
		)
		return output.NewWriter(task.PartitionID, collectPartitioner{}, idToOutput), nil
	}

	if partitions.IsPreserved(curStage.Output.Partitioner) {
		// only connect local
		nextTaskID := path.Join(runningJob.Job.ID, curStage.Output.Stage, task.PartitionID)
		nextTask := w.getRunningTask(nextTaskID)

		idToOutput[task.PartitionID] = NewLocalPipe(nextTask.Input)
		return output.NewWriter(task.PartitionID, partitions.NewPreservePartitioner(), idToOutput), nil
	}

	var mu sync.Mutex
	wg, wctx := errgroup.WithContext(runningJob.Context())
	for i, h := range taskExec.OutputDesc.PartitionToHost {
		id, host := i, h

		taskID := path.Join(runningJob.Job.ID, curStage.Output.Stage, id)
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
			conn, err := w.Cluster.Connect(wctx, host)
			if err != nil {
				return errors.Wrapf(err, "dial %s", host)
			}
			out, err := output.OpenPushStream(runningJob.Context(), lrmrpb.NewNodeClient(conn), w.Node.Info(), host, taskID)
			if err != nil {
				return errors.Wrapf(err, "connect %s", host)
			}
			mu.Lock()
			defer mu.Unlock()
			idToOutput[id] = output.NewBufferedOutput(out, w.opt.Output.BufferLength)
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	return output.NewWriter(task.PartitionID, partitions.UnwrapPartitioner(curStage.Output.Partitioner), idToOutput), nil
}

func (w *Executor) startTask(taskExec *TaskExecutor) {
	runningTasksGauge := lrmrmetric.RunningTasksGauge.With(lrmrmetric.WorkerLabelValuesFrom(w.Node.Info()))
	runningTasksGauge.Inc()
	defer runningTasksGauge.Dec()
	defer w.runningTasks.Delete(taskExec.task.ID().String())

	taskExec.Run()
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

	in := input.NewPushStream(exec.Input, stream)
	if err := in.Dispatch(); err != nil {
		return err
	}

	// upstream may have been closed, but that should not affect the task result
	_ = stream.SendAndClose(&pbtypes.Empty{})
	return nil
}

func (w *Executor) Close() error {
	w.Node.Unregister()
	w.RPCServer.GracefulStop()
	return nil
}

func errorLogMiddleware(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error { //nolint:lll
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

// Executor implements lrmrpb.NodeServer.
var _ lrmrpb.NodeServer = (*Executor)(nil)
