package executor

import (
	"context"
	"errors"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/lrmrpb"
	lrmrmetric "github.com/ab180/lrmr/metric"
	"go.uber.org/atomic"
)

type StatusReporter interface {
	JobContext() context.Context
	Collect([]*lrdd.Row) error
	ReportTaskSuccess(context.Context, job.TaskID, lrmrmetric.Metrics) error
	ReportTaskFailure(context.Context, job.TaskID, error, lrmrmetric.Metrics) error
}

type attachedStatusReporter struct {
	jobID      string
	stream     lrmrpb.Node_StartJobInForegroundServer
	outputChan chan *lrmrpb.JobOutput
	closed     atomic.Bool
}

func newAttachedStatusReporter(jobID string, stream lrmrpb.Node_StartJobInForegroundServer) StatusReporter {
	reporter := &attachedStatusReporter{
		jobID:      jobID,
		stream:     stream,
		outputChan: make(chan *lrmrpb.JobOutput),
	}
	go reporter.pipeChannelToStream()
	return reporter
}

// send emits lrmrpb.JobOutput into the stream, with protecting from channel closes.
func (f *attachedStatusReporter) send(ctx context.Context, out *lrmrpb.JobOutput) error {
	if f.closed.Load() {
		return errors.New("job is already completed")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case f.outputChan <- out:
		return nil
	}
}

// pipeChannelToStream fans in lrmrpb.JobOutput from multiple task executor goroutines into the stream,
// because writing to a gRPC stream is not concurrent-safe.
func (f *attachedStatusReporter) pipeChannelToStream() {
	defer f.closed.Store(true)
	for {
		select {
		case out := <-f.outputChan:
			if err := f.stream.Send(out); err != nil {
				log.Error("Failed to report status of job {}: {}", f.jobID, err)
			}
		case <-f.stream.Context().Done():
			close(f.outputChan)
			return
		}
	}
}

func (f *attachedStatusReporter) JobContext() context.Context {
	return f.stream.Context()
}

func (f *attachedStatusReporter) Collect(rows []*lrdd.Row) error {
	return f.send(f.stream.Context(), &lrmrpb.JobOutput{
		Type: lrmrpb.JobOutput_COLLECT_DATA,
		Data: rows,
	})
}

func (f *attachedStatusReporter) ReportTaskSuccess(ctx context.Context, taskID job.TaskID, metrics lrmrmetric.Metrics) error { //nolint:lll
	return f.send(ctx, &lrmrpb.JobOutput{
		Type:        lrmrpb.JobOutput_REPORT_TASK_COMPLETION,
		TaskStatus:  lrmrpb.JobOutput_SUCCEED,
		Stage:       taskID.StageName,
		PartitionID: taskID.PartitionID,
		Metrics:     metrics,
	})
}

func (f *attachedStatusReporter) ReportTaskFailure(ctx context.Context, taskID job.TaskID, taskErr error, metrics lrmrmetric.Metrics) error { //nolint:lll
	return f.send(ctx, &lrmrpb.JobOutput{
		Type:        lrmrpb.JobOutput_REPORT_TASK_COMPLETION,
		TaskStatus:  lrmrpb.JobOutput_FAILED,
		Stage:       taskID.StageName,
		PartitionID: taskID.PartitionID,
		Error:       taskErr.Error(),
		Metrics:     metrics,
	})
}

type detachedStatusReporter struct {
	jobManager job.Manager
	jobContext context.Context
}

func newDetachedStatusReporter(clusterState cluster.State, j *job.Job) StatusReporter {
	jobManager := job.NewDistributedManager(clusterState, j)

	ctx, cancel := context.WithCancel(context.Background())
	jobManager.OnJobCompletion(func(status *job.Status) {
		cancel()
	})
	return &detachedStatusReporter{
		jobManager: jobManager,
		jobContext: ctx,
	}
}

func (b *detachedStatusReporter) JobContext() context.Context {
	return b.jobContext
}

func (b *detachedStatusReporter) Collect(rows []*lrdd.Row) error {
	panic("collect not supported on detachedStatusReporter")
}

func (b *detachedStatusReporter) ReportTaskSuccess(ctx context.Context, id job.TaskID, metrics lrmrmetric.Metrics) error { //nolint:lll
	return b.jobManager.MarkTaskAsSucceed(ctx, id, metrics)
}

func (b *detachedStatusReporter) ReportTaskFailure(ctx context.Context, id job.TaskID, err error, metrics lrmrmetric.Metrics) error { //nolint:lll
	return b.jobManager.MarkTaskAsFailed(ctx, id, err, metrics)
}
