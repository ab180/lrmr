package worker

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/transformation"
	"github.com/vmihailenco/msgpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"net"
	"sync/atomic"
	"time"
)

type Worker struct {
	nodeManager node.Manager
	server      *grpc.Server

	contexts map[string]*workerJobContext

	log logger.Logger
	opt *Options
}

func New(crd coordinator.Coordinator, opt *Options) (*Worker, error) {
	nm, err := node.NewManager(crd, node.DefaultManagerOptions())
	if err != nil {
		return nil, err
	}
	return &Worker{
		nodeManager: nm,
		server:      grpc.NewServer(),
		contexts:    make(map[string]*workerJobContext),
		log:         logger.New("worker"),
		opt:         opt,
	}, nil
}

func (w *Worker) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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
		if err := msgpack.Unmarshal(broadcast, &val); err != nil {
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
		err := msgpack.Unmarshal(data, tf)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "recover transformation: %w", err)
		}
	}

	w.log.Info("Create task {} of {}/{} (Job: {})", task.ID, job.ID, stage.Name, job.Name)
	w.log.Info("  output: {}", req.Output.String())

	out, err := w.connectOutput(ctx, req.Output)
	if err != nil {
		// TODO: task removal
		return nil, status.Errorf(codes.Internal, "unable to connect output: %w", err)
	}

	c := &workerJobContext{
		job:            job,
		stage:          stage,
		transformation: tf,
		output:         out,
		broadcasts:     broadcasts,
		states:         make(map[string]interface{}),
	}
	if err := c.transformation.Setup(c); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to set up task")
	}
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
	var ctx *workerJobContext
	var taskRef node.TaskReference
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return w.abortTask(stream, taskRef, fmt.Errorf("stream recv: %v", err))
		}
		if ctx == nil {
			// first event of the stream. warm up
			ctx = w.contexts[req.TaskID]
			if ctx == nil {
				w.log.Info("no ctx")
				return status.Errorf(codes.NotFound, "task not found: %s", req.TaskID)
			}
			taskRef.TaskID = req.TaskID
			taskRef.StageName = ctx.stage.Name
			taskRef.JobID = ctx.job.ID
			atomic.AddInt32(&ctx.totalInputCounts, 1)

			w.log.Debug("Task {} ({}/{}) connected with {} ({})", req.TaskID, ctx.job.ID, ctx.stage.Name, req.From.Host, req.From.ID)

		} else if !ctx.isRunning {
			ctx.isRunning = true
			if err := w.nodeManager.UpdateTaskStatus(taskRef.String(), node.Running); err != nil {
				w.log.Error("Failed to change task status to running", err)
			}
		}
		for _, data := range req.Inputs {
			inputRow := make(lrdd.Row)
			if err := inputRow.Unmarshal(data); err != nil {
				return w.abortTask(stream, taskRef, fmt.Errorf("unmarshal row: %v", err))
			}
			if err := ctx.transformation.Run(inputRow, ctx.output); err != nil {
				return w.abortTask(stream, taskRef, fmt.Errorf("transformation: %v", err))
			}
		}
	}
	if ctx == nil {
		return status.Errorf(codes.InvalidArgument, "no events but found EOF")
	}
	atomic.AddInt32(&ctx.finishedInputCounts, 1)
	if ctx.finishedInputCounts >= ctx.totalInputCounts {
		// do finalize if all inputs have done
		return w.finishTask(taskRef, ctx, stream)
	}
	return stream.SendAndClose(&empty.Empty{})
}

func (w *Worker) finishTask(taskRef node.TaskReference, ctx *workerJobContext, stream lrmrpb.Worker_RunTaskServer) error {
	if err := ctx.output.Flush(); err != nil {
		return w.abortTask(stream, taskRef, fmt.Errorf("flush output: %v", err))
	}
	go func() {
		if err := ctx.output.Close(); err != nil {
			w.log.Error("error closing output", err)
		}
	}()
	if err := ctx.transformation.Teardown(); err != nil {
		return w.abortTask(stream, taskRef, fmt.Errorf("teardown: %v", err))
	}
	delete(w.contexts, taskRef.TaskID)

	if err := w.nodeManager.ReportTaskSuccess(taskRef); err != nil {
		return status.Errorf(codes.Internal, "task success report failed: %w", err)
	}
	return stream.SendAndClose(&empty.Empty{})
}

func (w *Worker) abortTask(stream lrmrpb.Worker_RunTaskServer, taskRef node.TaskReference, err error) error {
	w.log.Error("Task {} failed: {}", taskRef.String(), err)

	reportErr := w.nodeManager.ReportTaskFailure(taskRef, err)
	if reportErr != nil {
		return status.Errorf(codes.Internal, "task failure report failed: %v", reportErr)
	}
	return stream.SendAndClose(&empty.Empty{})
}

func (w *Worker) Stop() error {
	w.server.Stop()
	return w.nodeManager.UnregisterNode(w.nodeManager.Self().ID)
}
