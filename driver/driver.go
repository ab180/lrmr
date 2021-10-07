package driver

import (
	"context"
	"io"
	"path"
	"sync"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/input"
	"github.com/ab180/lrmr/internal/errchannel"
	"github.com/ab180/lrmr/internal/errgroup"
	"github.com/ab180/lrmr/internal/pbtypes"
	"github.com/ab180/lrmr/internal/serialization"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/lrmrpb"
	"github.com/ab180/lrmr/metric"
	"github.com/ab180/lrmr/output"
	"github.com/pkg/errors"
	"github.com/therne/errorist"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Driver interface {
	RunAttached(context.Context) (*CollectResult, error)
	RunDetached(context.Context) error
}

type CollectResult struct {
	Outputs []*lrdd.Row
	Metrics lrmrmetric.Metrics
}

type Remote struct {
	Job         *job.Job
	Cluster     cluster.Cluster
	Connections map[string]lrmrpb.NodeClient
	Input       input.Feeder
	Broadcast   serialization.Broadcast
}

func NewRemote(ctx context.Context, j *job.Job, c cluster.Cluster, in input.Feeder, broadcasts serialization.Broadcast) (Driver, error) {
	var (
		conns = make(map[string]lrmrpb.NodeClient)
		mu    sync.Mutex
	)
	var wg errgroup.Group
	for _, host := range j.Hostnames() {
		host := host
		wg.Go(func() error {
			conn, err := c.Connect(ctx, host)
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
		Cluster:     c,
		Connections: conns,
		Input:       in,
		Broadcast:   broadcasts,
	}, nil
}

func (m *Remote) RunAttached(ctx context.Context) (*CollectResult, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
			_ = stream.CloseSend()
		}
	}()

	// 3.1. monitor status
	// we use error channel for centralized lifecycle management
	var (
		activeStreamCnt = atomic.NewInt32(0)
		errChan         = errchannel.New()
		statusChan      = make(chan *lrmrpb.JobOutput)
	)
	defer errChan.Close()

	for host, stream := range streams {
		host, stream := host, stream
		activeStreamCnt.Add(1)
		go func() {
			defer func() {
				if err := errorist.WrapPanic(recover()); err != nil {
					errChan.Send(err)
				}
				cnt := activeStreamCnt.Dec()
				if cnt == 0 {
					close(statusChan)
				}
			}()

			for ctx.Err() == nil {
				msg, err := stream.Recv()
				if err != nil {
					if err == io.EOF || status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
						return
					}
					if status.Code(err) == codes.DeadlineExceeded {
						errChan.Send(context.DeadlineExceeded)
						return
					}
					errChan.Send(errors.Wrapf(err, "receive status from %s", host))
					return
				}
				if activeStreamCnt.Load() > 0 {
					statusChan <- msg
				}
			}
		}()
	}

	// 3.2. handle status
	jobMgr := job.NewLocalManager(m.Job)
	jobMgr.OnJobCompletion(func(s *job.Status) {
		if len(s.Errors) > 0 {
			errChan.Send(s.Errors[0])
			return
		}
		errChan.Send(nil)
	})

	var result []*lrdd.Row
JobRun:
	// 4. consume status stream
	for {
		select {
		// 4.1. received status report message from executor node
		case msg := <-statusChan:
			if msg == nil {
				// channel is closed before job completion
				continue
			}
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
					_ = jobMgr.MarkTaskAsFailed(ctx, taskID, errors.New(msg.Error), msg.Metrics)
					continue
				}
				_ = jobMgr.MarkTaskAsSucceed(ctx, taskID, msg.Metrics)
			}

		// 4.2. job is completed (success when err == nil)
		case err := <-errChan.Recv():
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
	metrics, err := jobMgr.CollectMetrics(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "collect metrics")
	}
	return &CollectResult{
		Outputs: result,
		Metrics: metrics,
	}, nil
}

func (m *Remote) RunDetached(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := m.createJob(ctx); err != nil {
		return errors.Wrap(err, "create job")
	}
	jobMgr := job.NewDistributedManager(m.Cluster.States(), m.Job)
	if _, err := jobMgr.RegisterStatus(ctx); err != nil {
		return errors.Wrap(err, "register job status")
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

func (m *Remote) feedInput(ctx context.Context, j *job.Job, in input.Feeder) (err error) {
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
	writer := output.NewWriter(input.FeederPartitionID, partitioner, outputsToFirstStage)
	defer errorist.CloseWithErrCapture(writer, &err, errorist.Wrapf("close"))

	if err := in.FeedInput(writer); err != nil {
		return errors.Wrap(err, "write data")
	}
	return nil
}
