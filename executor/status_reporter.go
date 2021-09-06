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

type foregroundJobStatusReporter struct {
	stream lrmrpb.Node_StartJobInForegroundServer
}

func newForegroundJobStatusReporter(stream lrmrpb.Node_StartJobInForegroundServer) StatusReporter {
	return &foregroundJobStatusReporter{stream: stream}
}

func (f *foregroundJobStatusReporter) JobContext() context.Context {
	return f.stream.Context()
}

func (f *foregroundJobStatusReporter) Collect(rows []*lrdd.Row) error {
	return f.stream.Send(&lrmrpb.JobOutput{
		Type: lrmrpb.JobOutput_COLLECT_DATA,
		Data: rows,
	})
}

func (f *foregroundJobStatusReporter) ReportTaskSuccess(ctx context.Context, taskID job.TaskID, metrics lrmrmetric.Metrics) error {
	return f.stream.Send(&lrmrpb.JobOutput{
		Type:        lrmrpb.JobOutput_REPORT_TASK_COMPLETION,
		TaskStatus:  lrmrpb.JobOutput_SUCCEED,
		Stage:       taskID.StageName,
		PartitionID: taskID.PartitionID,
		Metrics:     metrics,
	})
}

func (f *foregroundJobStatusReporter) ReportTaskFailure(ctx context.Context, taskID job.TaskID, taskErr error, metrics lrmrmetric.Metrics) error {
	return f.stream.Send(&lrmrpb.JobOutput{
		Type:        lrmrpb.JobOutput_REPORT_TASK_COMPLETION,
		TaskStatus:  lrmrpb.JobOutput_FAILED,
		Stage:       taskID.StageName,
		PartitionID: taskID.PartitionID,
		Error:       taskErr.Error(),
		Metrics:     metrics,
	})
}

type backgroundJobStatusReporter struct {
	statusManager job.StatusManager
	jobContext    context.Context
}

func newBackgroundJobStatusReporter(clusterState cluster.State, j *job.Job) StatusReporter {
	statusManager := job.NewDistributedStatusManager(clusterState, j)

	ctx, cancel := context.WithCancel(context.Background())
	statusManager.OnJobCompletion(func(status *job.Status) {
		cancel()
	})
	return &backgroundJobStatusReporter{
		statusManager: statusManager,
		jobContext:    ctx,
	}
}

func (b *backgroundJobStatusReporter) JobContext() context.Context {
	return b.jobContext
}

func (b *backgroundJobStatusReporter) Collect(rows []*lrdd.Row) error {
	panic("collect not supported on backgroundJobStatusReporter")
}

func (b *backgroundJobStatusReporter) ReportTaskSuccess(ctx context.Context, id job.TaskID, metrics lrmrmetric.Metrics) error {
	return b.statusManager.MarkTaskAsSucceed(ctx, id, metrics)
}

func (b *backgroundJobStatusReporter) ReportTaskFailure(ctx context.Context, id job.TaskID, err error, metrics lrmrmetric.Metrics) error {
	return b.statusManager.MarkTaskAsFailed(ctx, id, err, metrics)
}
