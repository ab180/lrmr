package driver

import (
	"context"
	"io"
	"path"
	"sync"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/input"
	"github.com/ab180/lrmr/internal/errgroup"
	"github.com/ab180/lrmr/internal/pbtypes"
	"github.com/ab180/lrmr/internal/serialization"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/lrmrpb"
	"github.com/ab180/lrmr/metric"
	"github.com/ab180/lrmr/output"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/errorist"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var log = logger.New("lrmr")

type Driver interface {
	RunSync(context.Context) (*CollectResult, error)
	RunAsync(context.Context) error
}

type CollectResult struct {
	Outputs []*lrdd.Row
	Metrics lrmrmetric.Metrics
}

type Remote struct {
	Job       *job.Job
	Cluster   cluster.Cluster
	Input     input.Feeder
	Broadcast serialization.Broadcast
}

func NewRemote(j *job.Job, c cluster.Cluster, in input.Feeder, broadcasts serialization.Broadcast) Driver {
	return &Remote{
		Job:       j,
		Cluster:   c,
		Input:     in,
		Broadcast: broadcasts,
	}
}

func (m *Remote) RunSync(ctx context.Context) (*CollectResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	marshalledJob := pbtypes.MustMarshalJSON(m.Job)
	serializedBroadcasts, err := serialization.SerializeBroadcast(m.Broadcast)
	if err != nil {
		return nil, errors.Wrap(err, "serialize broadcasts")
	}

	var (
		streams = make(map[string]lrmrpb.Node_RunJobInForegroundClient)
		rpcs    = make(map[string]lrmrpb.NodeClient)
		mu      sync.Mutex
	)

	statusChan := make(chan *lrmrpb.RunOnlineJobOutputToDriver)
	defer close(statusChan)

	// 1. connect nodes
	var wg errgroup.Group
	for host, stages := range m.Job.BuildStageDefinitionPerNode() {
		host, stages := host, stages

		wg.Go(func() error {
			conn, err := m.Cluster.Connect(ctx, host)
			if err != nil {
				return errors.Wrapf(err, "dial %s", host)
			}
			rpc := lrmrpb.NewNodeClient(conn)
			stream, err := rpc.RunJobInForeground(ctx, &lrmrpb.RunJobRequest{
				Job:        marshalledJob,
				Stages:     stages,
				Broadcasts: serializedBroadcasts,
			})
			if err != nil {
				return errors.Wrapf(err, "call RunOnlineJob to %s", host)
			}
			mu.Lock()
			defer mu.Unlock()

			streams[host] = stream
			rpcs[host] = rpc

			// wait for tasks to be created
			msg, err := stream.Recv()
			if err != nil {
				return errors.Wrap(err, "received error while waiting for task creation")
			}
			if msg.Type != lrmrpb.RunOnlineJobOutputToDriver_TASKS_READY {
				return errors.Errorf("invalid status order: expected TASKS_READY but received %s", msg.Type.String())
			}
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	defer func() {
		for _, stream := range streams {
			stream.CloseSend()
		}
	}()

	// 2. feed inputs
	if err := m.feedInput(ctx, m.Job, m.Input); err != nil {
		return nil, errors.Wrap(err, "feed input")
	}

	// 3.1. monitor status
	// we use error channel for centralized lifecycle management
	errChan := make(chan error)
	for host, stream := range streams {
		host, stream := host, stream
		go func() {
			for ctx.Err() == nil {
				msg, err := stream.Recv()
				if err != nil {
					if err == io.EOF || status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
						return
					}
					errChan <- errors.Wrapf(err, "receive status from %s", host)
					return
				}
				statusChan <- msg
			}
		}()
	}

	// 3.2. handle status
	stateMgr := job.NewLocalStatusManager(m.Job)
	stateMgr.OnJobCompletion(func(s *job.Status) {
		var err error
		if len(s.Errors) > 0 {
			err = s.Errors[0]
		}
		select {
		case errChan <- err:
		default:
		}
	})

	var result []*lrdd.Row
JobRun:
	// 4. consume status stream
	for {
		select {
		// 4.1. received status report message from executor node
		case msg := <-statusChan:
			switch msg.Type {
			case lrmrpb.RunOnlineJobOutputToDriver_COLLECT_DATA:
				result = append(result, msg.Data...)

			case lrmrpb.RunOnlineJobOutputToDriver_REPORT_TASK_COMPLETION:
				log.Verbose("REPORT_TASK_COMPLETION received: {}/{}/{}", m.Job.ID, msg.Stage, msg.PartitionID)
				taskID := job.TaskID{
					JobID:       m.Job.ID,
					StageName:   msg.Stage,
					PartitionID: msg.PartitionID,
				}
				if msg.TaskStatus == lrmrpb.RunOnlineJobOutputToDriver_FAILED {
					_ = stateMgr.MarkTaskAsFailed(ctx, taskID, errors.New(msg.Error))
					continue
				}
				_ = stateMgr.MarkTaskAsSucceed(ctx, taskID)
			}

		// 4.2. job is completed (success when err == nil)
		case err := <-errChan:
			if err != nil {
				return nil, err
			}
			break JobRun

		// 4.3. context cancelled
		case <-ctx.Done():
			// at the moment, the job is already cancelled since RunJob stream is bound to ctx
			return nil, ctx.Err()
		}
	}
	return &CollectResult{
		Outputs: result,
		Metrics: nil,
	}, nil
}

func (m *Remote) RunAsync(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	serializedJob := pbtypes.MustMarshalJSON(m.Job)
	serializedBroadcasts, err := serialization.SerializeBroadcast(m.Broadcast)
	if err != nil {
		return errors.Wrap(err, "serialize broadcasts")
	}

	wg, wctx := errgroup.WithContext(ctx)
	for host, stages := range m.Job.BuildStageDefinitionPerNode() {
		host, stages := host, stages

		wg.Go(func() error {
			conn, err := m.Cluster.Connect(wctx, host)
			if err != nil {
				return errors.Wrapf(err, "dial %s", host)
			}
			rpc := lrmrpb.NewNodeClient(conn)
			req := &lrmrpb.RunJobRequest{
				Job:        serializedJob,
				Stages:     stages,
				Broadcasts: serializedBroadcasts,
			}
			if _, err := rpc.RunJobInBackground(wctx, req); err != nil {
				return errors.Wrapf(err, "call CreateTask on %s", host)
			}
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return err
	}
	if err := m.feedInput(ctx, m.Job, m.Input); err != nil {
		return errors.Wrap(err, "feed input")
	}
	return nil
}

func (m *Remote) feedInput(ctx context.Context, j *job.Job, input input.Feeder) (err error) {
	// feed input to first stage (ignore stage #0 because it is input stage itself)
	firstStage, targets := j.Stages[1], j.Partitions[1]
	partitioner := j.Stages[0].Output.Partitioner

	outputsToFirstStage := make(map[string]output.Output, len(targets))
	var (
		lock sync.Mutex
		wg   errgroup.Group
	)
	for _, t := range targets {
		assigned := t
		wg.Go(func() error {
			taskID := path.Join(j.ID, firstStage.Name, assigned.PartitionID)
			conn, err := m.Cluster.Connect(ctx, assigned.Host)
			if err != nil {
				return errors.Wrapf(err, "dial %s", assigned.Host)
			}
			out, err := output.OpenPushStream(ctx, lrmrpb.NewNodeClient(conn), nil, assigned.Host, taskID)
			if err != nil {
				return errors.Wrapf(err, "connect %s", assigned.Host)
			}
			lock.Lock()
			outputsToFirstStage[assigned.PartitionID] = out
			lock.Unlock()
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return err
	}
	writer := output.NewWriter("0", partitioner, outputsToFirstStage)
	defer errorist.CloseWithErrCapture(writer, &err, errorist.Wrapf("close"))

	if err := input.FeedInput(writer); err != nil {
		return errors.Wrap(err, "write data")
	}
	return nil
}

func (m *Remote) collectMetrics(ctx context.Context, j *job.Job, rpcs map[string]lrmrpb.NodeClient) lrmrmetric.Metrics {
	metric := make(lrmrmetric.Metrics)
	for _, rpc := range rpcs {
		resp, err := rpc.GetMetric(ctx, &lrmrpb.GetMetricRequest{
			JobID: j.ID,
		})
		if err != nil {
			log.Error("Failed to collect metrics: {}", err)
			return lrmrmetric.Metrics{}
		}
		metric = metric.Sum(resp.Metrics)
	}
	return metric
}
