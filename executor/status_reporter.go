package executor

import (
	"context"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/lrmrpb"
	lrmrmetric "github.com/ab180/lrmr/metric"
)

type StatusReporter interface {
	JobContext() context.Context
	Collect([]*lrdd.Row) error
	ReportTaskSuccess(context.Context, job.TaskID, lrmrmetric.Metrics) error
	ReportTaskFailure(context.Context, job.TaskID, error, lrmrmetric.Metrics) error
}

type attachedStatusReporter struct {
	stream lrmrpb.Node_StartJobInForegroundServer
}

func newAttachedStatusReporter(stream lrmrpb.Node_StartJobInForegroundServer) StatusReporter {
	return &attachedStatusReporter{stream: stream}
}

func (f *attachedStatusReporter) JobContext() context.Context {
	return f.stream.Context()
}

func (f *attachedStatusReporter) Collect(rows []*lrdd.Row) error {
	return f.stream.Send(&lrmrpb.JobOutput{
		Type: lrmrpb.JobOutput_COLLECT_DATA,
		Data: rows,
	})
}

func (f *attachedStatusReporter) ReportTaskSuccess(ctx context.Context, taskID job.TaskID, metrics lrmrmetric.Metrics) error {
	return f.stream.Send(&lrmrpb.JobOutput{
		Type:        lrmrpb.JobOutput_REPORT_TASK_COMPLETION,
		TaskStatus:  lrmrpb.JobOutput_SUCCEED,
		Stage:       taskID.StageName,
		PartitionID: taskID.PartitionID,
		Metrics:     metrics,
	})
}

func (f *attachedStatusReporter) ReportTaskFailure(ctx context.Context, taskID job.TaskID, taskErr error, metrics lrmrmetric.Metrics) error {
	return f.stream.Send(&lrmrpb.JobOutput{
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

func (b *detachedStatusReporter) ReportTaskSuccess(ctx context.Context, id job.TaskID, metrics lrmrmetric.Metrics) error {
	return b.jobManager.MarkTaskAsSucceed(ctx, id, metrics)
}

func (b *detachedStatusReporter) ReportTaskFailure(ctx context.Context, id job.TaskID, err error, metrics lrmrmetric.Metrics) error {
	return b.jobManager.MarkTaskAsFailed(ctx, id, err, metrics)
}
