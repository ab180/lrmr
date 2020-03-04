package worker

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/shamaton/msgpack"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"net"
	"runtime/debug"
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

type connection struct {
	stream     lrmrpb.Worker_RunTaskServer
	executor   *executorHandle
	fromHost   string
	fromNodeID string
}

func New(crd coordinator.Coordinator, opt *Options) (*Worker, error) {
	nm, err := node.NewManager(crd, node.DefaultManagerOptions())
	if err != nil {
		return nil, err
	}
	return &Worker{
		nodeManager:     nm,
		jobReporter:     node.NewJobReporter(crd),
		server:          grpc.NewServer(grpc.MaxRecvMsgSize(1 << 25)),
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
	stage := job.GetStage(req.Stage.Name)
	if stage == nil {
		return nil, status.Errorf(codes.InvalidArgument, "stage %s not found on job %s", req.Stage.Name, req.Stage.JobID)
	}

	broadcasts := make(map[string]interface{})
	for key, broadcast := range req.Broadcasts {
		var val interface{}
		if err := msgpack.Decode(broadcast, &val); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "unable to unmarshal broadcast %s: %w", key, err)
		}
		broadcasts[key] = val
	}

	task := node.NewTask(w.nodeManager.Self(), job, stage)
	if err := w.nodeManager.CreateTask(ctx, task); err != nil {
		return nil, status.Errorf(codes.Internal, "create task failed: %w", err)
	}

	// restore transformation object from broadcasts
	tf := transformation.Lookup(req.Stage.Transformation)
	if data, ok := broadcasts["__stage/"+req.Stage.Name].([]byte); ok {
		err := msgpack.Decode(data, tf)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "recover transformation: %w", err)
		}
	}

	log.Info("Create task {} of {}/{} (Job: {})", task.ID, job.ID, stage.Name, job.Name)
	log.Info("  output: {}", req.Output.String())

	out, err := w.connectOutput(ctx, req.Output)
	if err != nil {
		// TODO: task removal
		return nil, status.Errorf(codes.Internal, "unable to connect output: %w", err)
	}

	c := &taskContext{
		worker:         w,
		job:            job,
		stage:          stage,
		task:           task,
		transformation: tf,
		output:         out,
		broadcasts:     broadcasts,
		workerCfgs:     w.workerLocalOpts,
		states:         make(map[string]interface{}),
	}
	if err := c.transformation.Setup(c); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to set up task")
	}
	c.executors = launchExecutorPool(tf, c, out, w.opt.Concurrency, w.opt.QueueLength)
	w.contexts[task.ID] = c
	return &lrmrpb.CreateTaskResponse{TaskID: task.ID}, nil
}

func (w *Worker) connectOutput(ctx context.Context, outDesc *lrmrpb.Output) (out output.Output, err error) {
	out, err = output.NewFromDesc(outDesc)
	if err != nil {
		return
	}
	err = out.Connect(ctx, w.nodeManager.Self(), outDesc)
	return
}

func (w *Worker) RunTask(stream lrmrpb.Worker_RunTaskServer) error {
	var ctx *taskContext
	var conn connection

	defer w.tryRecover(ctx, &conn)

	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return w.abortTask(ctx, conn, fmt.Errorf("stream recv: %v", err))
		}
		if ctx == nil {
			// first event of the stream. warm up
			ctx = w.contexts[req.TaskID]
			if ctx == nil {
				return status.Errorf(codes.NotFound, "task not found: %s", req.TaskID)
			}
			atomic.AddInt32(&ctx.totalInputCounts, 1)

			conn.executor = ctx.executors.NewExecutorHandle()
			go func() {
				// report errors in executors
				for err := range conn.executor.Errors {
					_ = w.abortTask(ctx, conn, err)
				}
			}()
			conn.fromHost = req.From.Host
			conn.fromNodeID = req.From.ID
			conn.stream = stream

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
				return w.abortTask(ctx, conn, fmt.Errorf("unmarshal row: %v", err))
			}
			conn.executor.Enqueue(inputRow)
		}
	}
	if ctx == nil {
		return status.Errorf(codes.InvalidArgument, "no events but found EOF")
	}
	conn.executor.WaitForCompletion()

	atomic.AddInt32(&ctx.finishedInputCounts, 1)
	if ctx.finishedInputCounts >= ctx.totalInputCounts {
		// do finalize if all inputs have done
		return w.finishTask(ctx, conn)
	}
	return stream.SendAndClose(&empty.Empty{})
}

func (w *Worker) finishTask(ctx *taskContext, conn connection) error {
	log.Info("Task {} finished. Closing... ", ctx.task.Reference())
	//ctx.executors.WaitForCompletion()

	if err := ctx.transformation.Teardown(ctx); err != nil {
		return w.abortTask(ctx, conn, fmt.Errorf("teardown: %v", err))
	}
	if err := ctx.output.Flush(); err != nil {
		return w.abortTask(ctx, conn, fmt.Errorf("flush output: %v", err))
	}
	go func() {
		if err := ctx.output.Close(); err != nil {
			log.Error("error closing output", err)
		}
	}()
	delete(w.contexts, ctx.task.ID)

	if err := w.jobReporter.ReportSuccess(ctx.task.Reference()); err != nil {
		log.Error("Task {} have been successfully done, but failed to report: {}", ctx.task.Reference(), err)
		return status.Errorf(codes.Internal, "task success report failed: %w", err)
	}
	return conn.stream.SendAndClose(&empty.Empty{})
}

func (w *Worker) tryRecover(ctx *taskContext, conn *connection) {
	if r := recover(); r != nil {
		err := fmt.Errorf("panic: %v\n%s", r, string(debug.Stack()))
		_ = w.abortTask(ctx, *conn, err)
	}
}

func (w *Worker) abortTask(ctx *taskContext, conn connection, err error) error {
	ctx.executors.Cancel()

	reportErr := w.jobReporter.ReportFailure(ctx.task.Reference(), err)
	if reportErr != nil {
		log.Error("Task {} failed with error: {}", ctx.task.Reference().String(), err)
		log.Error("While reporting the error, another error occurred", err)
		return status.Errorf(codes.Internal, "task failure report failed: %v", reportErr)
	}
	log.Error("  Caused by connection from {} ({})", conn.fromHost, conn.fromNodeID)
	return conn.stream.SendAndClose(&empty.Empty{})
}

func (w *Worker) Stop() error {
	w.server.Stop()
	w.jobReporter.Close()
	return w.nodeManager.UnregisterNode(w.nodeManager.Self().ID)
}
