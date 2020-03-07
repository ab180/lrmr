package worker

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	"github.com/shamaton/msgpack"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/internal/logutils"
	"github.com/therne/lrmr/lrdd"
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
	"sync/atomic"
	"time"
)

var log = logger.New("worker")

type Worker struct {
	nodeManager node.Manager
	jobReporter *node.JobReporter
	server      *grpc.Server

	contexts        map[string]*taskContext
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
			grpc_recovery.UnaryServerInterceptor(
				grpc_recovery.WithRecoveryHandler(func(r interface{}) error {
					err := logutils.WrapRecover(r)
					log.Error(err.Pretty())
					return status.Errorf(codes.Internal, err.Error())
				}),
			),
		)),
	)

	return &Worker{
		nodeManager:     nm,
		jobReporter:     node.NewJobReporter(crd),
		server:          srv,
		contexts:        make(map[string]*taskContext),
		workerLocalOpts: make(map[string]interface{}),
		opt:             opt,
	}, nil
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

func (w *Worker) SetWorkerLocalOption(key string, value interface{}) {
	w.workerLocalOpts[key] = value
}

func (w *Worker) CreateTask(ctx context.Context, req *lrmrpb.CreateTaskRequest) (*lrmrpb.CreateTaskResponse, error) {
	job, err := w.nodeManager.GetJob(ctx, req.Stage.JobID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get job info: %w", err)
	}
	s := job.GetStage(req.Stage.Name)
	if s == nil {
		return nil, status.Errorf(codes.InvalidArgument, "stage %s not found on job %s", req.Stage.Name, req.Stage.JobID)
	}
	st := stage.Lookup(req.Stage.RunnerName)
	runner := st.Constructor()

	broadcasts := make(map[string]interface{})
	for key, broadcast := range req.Broadcasts {
		var val interface{}
		if err := msgpack.Decode(broadcast, &val); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "unable to unmarshal broadcast %s: %w", key, err)
		}
		broadcasts[key] = val
	}

	// restore runner object contents from broadcasts
	if data, ok := broadcasts["__stage/"+req.Stage.Name].([]byte); ok {
		err := msgpack.Decode(data, runner)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "recover transformation: %w", err)
		}
	}

	shards, err := output.DialShards(ctx, w.nodeManager.Self(), req.Output, w.opt.Output)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "unable to connect output: %w", err)
	}

	task := node.NewTask(w.nodeManager.Self(), job, s)
	ts, err := w.nodeManager.CreateTask(ctx, task)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create task failed: %w", err)
	}
	w.jobReporter.Add(task.Reference(), ts)

	log.Info("Create task {} of {}/{} (Job: {})", task.ID, job.ID, s.Name, job.Name)
	log.Info("  output: {}", req.Output.String())

	c := &taskContext{
		worker:     w,
		job:        job,
		stage:      s,
		task:       task,
		runner:     runner,
		shards:     shards,
		broadcasts: broadcasts,
		workerCfgs: w.workerLocalOpts,
		states:     make(map[string]interface{}),
	}
	c.executors = launchExecutorPool(runner, c, shards, w.opt.Concurrency, w.opt.QueueLength)
	w.contexts[task.ID] = c

	if err := c.runner.Setup(c); err != nil {
		c.executors.Cancel()
		_ = w.jobReporter.ReportFailure(task.Reference(), err)
		delete(w.contexts, task.ID)

		return nil, status.Errorf(codes.Internal, "failed to set up task")
	}
	return &lrmrpb.CreateTaskResponse{TaskID: task.ID}, nil
}

func (w *Worker) RunTask(stream lrmrpb.Worker_RunTaskServer) error {
	var ctx *taskContext
	var onceBackpressure sync.Once

	conn := newConnection(stream, w.jobReporter)
	defer conn.TryRecover()

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			if grpcErr, ok := status.FromError(err); ok {
				if grpcErr.Code() == codes.ResourceExhausted {
					// incoming stream speed is too faster than processing speed
					onceBackpressure.Do(func() {
						conn.log.Warn("Stream backpressure detected. waiting for stream queues to be drained.")
					})
					conn.WaitForCompletion()
					continue
				}
			}
			return conn.AbortTask(fmt.Errorf("stream recv: %v", err))
		}
		if ctx == nil {
			// first event of the stream. warm up
			ctx = w.contexts[req.TaskID]
			if ctx == nil {
				return status.Errorf(codes.NotFound, "task not found: %s", req.TaskID)
			}
			atomic.AddInt32(&ctx.totalInputCounts, 1)

			conn.SetContext(ctx, req.From)
			ctx.addConnection(conn)

			log.Debug("Task {} ({}/{}) connected with {} ({})", req.TaskID, ctx.job.ID, ctx.stage.Name, req.From.Host, req.From.ID)

		} else if !ctx.isRunning {
			ctx.isRunning = true
			w.jobReporter.UpdateStatus(ctx.task.Reference(), func(ts *node.TaskStatus) {
				ts.Status = node.Running
			})
		}
		for _, data := range req.Inputs {
			inputRow := make(lrdd.Row)
			if err := inputRow.Unmarshal(data); err != nil {
				return conn.AbortTask(fmt.Errorf("unmarshal row: %v", err))
			}
			conn.Enqueue(inputRow)
		}
	}
	if ctx == nil {
		return status.Errorf(codes.InvalidArgument, "no events but found EOF")
	}
	conn.WaitForCompletion()

	atomic.AddInt32(&ctx.finishedInputCounts, 1)
	if ctx.finishedInputCounts >= ctx.totalInputCounts {
		// do finalize if all inputs have done
		log.Info("Task {} finished. Closing... ", ctx.task.Reference())
		for _, c := range conn.ctx.connections {
			c.WaitForCompletion()
		}
		delete(w.contexts, ctx.task.ID)
		return conn.FinishTask()
	}
	return stream.SendAndClose(&empty.Empty{})
}

func (w *Worker) Stop() error {
	w.server.Stop()
	w.jobReporter.Close()
	return w.nodeManager.UnregisterNode(w.nodeManager.Self().ID)
}
