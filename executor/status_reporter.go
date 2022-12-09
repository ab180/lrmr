package executor

import (
	"context"
	"sync"

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
	mu     *sync.Mutex
	jobID  string
	stream lrmrpb.Node_StartJobInForegroundServer
}

func newAttachedStatusReporter(jobID string, stream lrmrpb.Node_StartJobInForegroundServer) StatusReporter {
	reporter := &attachedStatusReporter{
		mu:     &sync.Mutex{},
		jobID:  jobID,
		stream: stream,
	}
	return reporter
}

// send emits lrmrpb.JobOutput into the stream, with protecting from channel closes.
func (f *attachedStatusReporter) send(ctx context.Context, out *lrmrpb.JobOutput) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	sent := make(chan error)
	go func() {
		defer close(sent)
		err := f.stream.Send(out)
		sent <- err
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-sent:
		return err
	}
}

func (f *attachedStatusReporter) JobContext() context.Context {
	return f.stream.Context()
}

func (f *attachedStatusReporter) Collect(marshalUnmarshalerRows []*lrdd.Row) error {
	if len(marshalUnmarshalerRows) == 0 {
		return nil
	}

	rowType := marshalUnmarshalerRows[0].Value.Type()

	rows := make([]*lrdd.RawRow, len(marshalUnmarshalerRows))
	for i, marshalUnmarshalerRow := range marshalUnmarshalerRows {
		val, err := marshalUnmarshalerRow.Value.MarshalMsg(nil)
		if err != nil {
			return err
		}

		rows[i] = &lrdd.RawRow{
			Key:   marshalUnmarshalerRow.Key,
			Value: val,
		}
	}

	return f.send(f.stream.Context(), &lrmrpb.JobOutput{
		Type:    lrmrpb.JobOutput_COLLECT_DATA,
		RowType: int32(rowType),
		Data:    rows,
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
