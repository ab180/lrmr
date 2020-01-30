package remote

import (
	"context"
	"github.com/airbloc/logger"
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/dataframe"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/vmihailenco/msgpack"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync/atomic"
)

type Worker struct {
	contexts map[string]*workerJobContext

	log logger.Logger
	opt lrmr.Options
}

func (w *Worker) Start() error {
	return nil
}

func (w *Worker) Prepare(ctx context.Context, req *lrmrpb.PrepareRequest) (*lrmrpb.EmptyResponse, error) {
	outputChans := make(map[string]chan interface{})
	hasConnection := make(map[string]bool)

	for key, host := range req.Outputs {
		outputChans[key] = make(chan interface{}, w.opt.OutputChannelSize)
		if !hasConnection[host] {
			if err := w.connectOutput(ctx, req.Next, host, outputChans[key]); err != nil {
				w.log.Error("unable to dial {}: {}", host, err)
				continue
			}
			hasConnection[host] = true
		}
	}

	broadcasts := make(map[string]interface{})
	for key, broadcast := range req.Broadcasts {
		var val interface{}
		if err := msgpack.Unmarshal(broadcast, &val); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "unable to unmarshal broadcast %s: %w", key, err)
		}
		broadcasts[key] = val
	}

	c := &workerJobContext{
		task:        lrmr.LookupTaskBy(req.Current.TaskName),
		outputChans: outputChans,
		broadcasts:  broadcasts,
		states:      make(map[string]interface{}),

		totalInputCounts: int32(len(req.Inputs)),
	}
	if err := c.task.Setup(c); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to set up task")
	}
	w.contexts[req.Current.TaskName+"/"+req.Current.JobID] = c

	return &lrmrpb.EmptyResponse{}, nil
}

func (w *Worker) connectOutput(ctx context.Context, nextStage *lrmrpb.Stage, host string, outputChan chan interface{}) (err error) {
	conn, err := grpc.DialContext(ctx, host)
	if err != nil {
		return
	}
	client := lrmrpb.NewNodeClient(conn)
	stream, err := client.Do(ctx)
	if err != nil {
		return
	}
	go func() {
		buffer := make([]*lrmrpb.Input, 0, w.opt.OutputBufferSize)
		for output := range outputChan {
			outputData := output.(*dataframe.Row)
			buffer = append(buffer, &lrmrpb.Input{Data: outputData.Marshal()})
			if len(buffer) == cap(buffer) {
				err := stream.Send(&lrmrpb.DoRequest{
					Stage:  nextStage,
					Inputs: buffer,
				})
				if err != nil {
					w.log.Error("TODO: failed to write stream: {}", err)
					continue
				}
				// this clears array length without triggering GC
				buffer = buffer[:0]
				continue
			}
		}
		if err := stream.CloseSend(); err != nil {
			// TODO: handle close error
			w.log.Error("TODO: failed to close stream: {}", err)
		}
	}()
	return
}

func (w *Worker) Do(stream lrmrpb.Node_DoServer) error {
	var ctx *workerJobContext
	var ctxID string
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			// TODO: fault tolerance
			return status.Errorf(codes.Internal, "stream recv: %v", err)
		}
		if ctx == nil {
			// must be first event of the stream
			ctxID = req.Stage.TaskName + "/" + req.Stage.JobID
			ctx = w.contexts[ctxID]
			if ctx == nil {
				return status.Errorf(codes.NotFound, "job not found: %s", req.Stage.JobID)
			}
		}
		for _, input := range req.Inputs {
			inputRow := make(dataframe.Row)
			if err := inputRow.Unmarshal(input.Data); err != nil {
				w.log.Error("task {} failed: unmarshal row: {}", req.Stage.TaskName, err)
				continue
			}
			if err := ctx.task.Run(inputRow, ctx.outputChans); err != nil {
				w.log.Error("task {} failed: run task: {}", req.Stage.TaskName, err)
			}
		}
	}
	if ctx == nil {
		return status.Errorf(codes.InvalidArgument, "no events but found EOF")
	}

	// do finalize if all inputs have done
	atomic.AddInt32(&ctx.finishedInputCounts, 1)
	if ctx.finishedInputCounts >= ctx.totalInputCounts {
		err := ctx.task.Teardown()
		if err != nil {
			// TODO: report failure
		} else {
			// TODO: report success
		}
		delete(w.contexts, ctxID)
	}
	return stream.SendAndClose(&lrmrpb.EmptyResponse{})
}

func (w *Worker) Stop() error {
	return nil
}
