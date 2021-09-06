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
	CollectMetrics(context.Context) (lrmrmetric.Metrics, error)
}

type CollectResult struct {
	Outputs []*lrdd.Row
	Metrics lrmrmetric.Metrics
}

type Remote struct {
	Job         *job.Job
	Connections map[string]lrmrpb.NodeClient
	Input       input.Feeder
	Broadcast   serialization.Broadcast
}

func NewRemote(ctx context.Context, j *job.Job, c cluster.Cluster, in input.Feeder, broadcasts serialization.Broadcast) (Driver, error) {
	var (
		conns = make(map[string]lrmrpb.NodeClient)
		mu    sync.Mutex
	)
	wg, wctx := errgroup.WithContext(ctx)
	for _, host := range j.Hostnames() {
		host := host
		wg.Go(func() error {
			conn, err := c.Connect(wctx, host)
			if err != nil {
				return errors.Wrapf(err, "dial %s", host)
			}
			mu.Lock()
			conns[host] = lrmrpb.NewNodeClient(conn)
			mu.Unlock()
			return nil
		})
	}
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	return &Remote{
		Job:         j,
		Connections: conns,
		Input:       in,
		Broadcast:   broadcasts,
	}, nil
}

func (m *Remote) RunSync(ctx context.Context) (*CollectResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	statusChan := make(chan *lrmrpb.JobOutput)
	defer close(statusChan)

	// 1. create the job in executors
	if err := m.createJob(ctx); err != nil {
		return nil, errors.Wrap(err, "create job")
	}

	// 2. start job and connect streams
	var (
		streams = make(map[string]lrmrpb.Node_StartJobInForegroundClient)
		mu      sync.Mutex
		wg      errgroup.Group
	)
	for host, rpc := range m.Connections {
		host, rpc := host, rpc

		wg.Go(func() error {
			req := &lrmrpb.StartJobRequest{
				JobID: m.Job.ID,
			}
			stream, err := rpc.StartJobInForeground(ctx, req)
			if err != nil {
				return errors.Wrapf(err, "call StartJobInForeground to %s", host)
			}
			mu.Lock()
			streams[host] = stream
			mu.Unlock()
			return nil
		})
	}
	// 2.1. feed inputs simultaneously
	wg.Go(func() error {
		return m.feedInput(ctx, m.Job, m.Input)
	})
	if err := wg.Wait(); err != nil {
		return nil, err
	}
	defer func() {
		for _, stream := range streams {
			stream.CloseSend()
		}
	}()

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
					select {
					case errChan <- errors.Wrapf(err, "receive status from %s", host):
					default:
					}
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
			case lrmrpb.JobOutput_COLLECT_DATA:
				result = append(result, msg.Data...)

			case lrmrpb.JobOutput_REPORT_TASK_COMPLETION:
				taskID := job.TaskID{
					JobID:       m.Job.ID,
					StageName:   msg.Stage,
					PartitionID: msg.PartitionID,
				}
				if msg.TaskStatus == lrmrpb.JobOutput_FAILED {
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

	if err := m.createJob(ctx); err != nil {
		return errors.Wrap(err, "create job")
	}
	wg, wctx := errgroup.WithContext(ctx)
	for _, rpc := range m.Connections {
		rpc := rpc
		wg.Go(func() error {
			req := &lrmrpb.StartJobRequest{
				JobID: m.Job.ID,
			}
			_, err := rpc.StartJobInBackground(wctx, req)
			return err
		})
	}
	if err := wg.Wait(); err != nil {
		return errors.Wrap(err, "start job")
	}
	if err := m.feedInput(ctx, m.Job, m.Input); err != nil {
		return errors.Wrap(err, "feed input")
	}
	return nil
}

func (m *Remote) createJob(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	serializedJob := pbtypes.MustMarshalJSON(m.Job)
	serializedBroadcasts, err := serialization.SerializeBroadcast(m.Broadcast)
	if err != nil {
		return errors.Wrap(err, "serialize broadcasts")
	}
	stagesProtoPerHost := m.Job.BuildStageDefinitionPerNode()

	wg, wctx := errgroup.WithContext(ctx)
	for host, rpc := range m.Connections {
		host, rpc := host, rpc
		stages := stagesProtoPerHost[host]

		wg.Go(func() error {
			req := &lrmrpb.CreateJobRequest{
				Job:        serializedJob,
				Stages:     stages,
				Broadcasts: serializedBroadcasts,
			}
			if _, err := rpc.CreateJob(wctx, req); err != nil {
				return errors.Wrapf(err, "call CreateTask on %s", host)
			}
			return nil
		})
	}
	return wg.Wait()
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
		rpc := m.Connections[assigned.Host]

		wg.Go(func() error {
			taskID := path.Join(j.ID, firstStage.Name, assigned.PartitionID)
			out, err := output.OpenPushStream(ctx, rpc, nil, assigned.Host, taskID)
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

func (m *Remote) CollectMetrics(ctx context.Context) (lrmrmetric.Metrics, error) {
	metric := make(lrmrmetric.Metrics)
	for _, rpc := range m.Connections {
		resp, err := rpc.GetMetric(ctx, &lrmrpb.GetMetricRequest{
			JobID: m.Job.ID,
		})
		if err != nil {
			return lrmrmetric.Metrics{}, err
		}
		metric = metric.Sum(resp.Metrics)
	}
	return metric, nil
}
