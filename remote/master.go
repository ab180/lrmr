package remote

import (
	"context"
	"github.com/airbloc/logger"
	"github.com/therne/lrmr"
	"github.com/therne/lrmr/dataframe"
	"github.com/therne/lrmr/lrmrpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync/atomic"
)

type Master struct {
	contexts      map[string]*masterContext
	subscriptions map[string]chan []dataframe.Row

	log logger.Logger
	opt lrmr.Options
}

func (m *Master) Start() error {
	return nil
}

func (m *Master) SubscribeResult(jobID string) chan []dataframe.Row {
	resultsChan := make(chan []dataframe.Row)
	m.subscriptions[jobID] = resultsChan
	return resultsChan
}

func (m *Master) Prepare(ctx context.Context, req *lrmrpb.PrepareRequest) (*lrmrpb.EmptyResponse, error) {
	c := &masterContext{
		totalInputCounts: int32(len(req.Inputs)),
	}
	m.contexts[req.Current.JobID] = c
	return &lrmrpb.EmptyResponse{}, nil
}

func (m *Master) Do(stream lrmrpb.Node_DoServer) error {
	var ctx *masterContext
	var jobID string
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
			jobID = req.Stage.JobID
			ctx = m.contexts[jobID]
			if ctx == nil {
				return status.Errorf(codes.NotFound, "job not found: %s", req.Stage.JobID)
			}
		}
		for _, input := range req.Inputs {
			row := make(dataframe.Row)
			if err := row.Unmarshal(input.Data); err != nil {
				m.log.Error("task {} failed: unmarshal row: {}", req.Stage.TaskName, err)
				continue
			}
			ctx.results = append(ctx.results, row)
		}
	}
	if ctx == nil {
		return status.Errorf(codes.InvalidArgument, "no events but found EOF")
	}

	// do finalize if all inputs have done
	atomic.AddInt32(&ctx.finishedInputCounts, 1)
	if ctx.finishedInputCounts >= ctx.totalInputCounts {
		m.subscriptions[jobID] <- ctx.results

		delete(m.contexts, jobID)
		delete(m.subscriptions, jobID)
	}
	return stream.SendAndClose(&lrmrpb.EmptyResponse{})
}

func (m *Master) Stop() error {
	return nil
}
