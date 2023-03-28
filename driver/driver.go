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
	"github.com/ab180/lrmr/output"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/therne/errorist"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Driver interface {
	RunAttached(context.Context) (Result, error)
	RunDetached(context.Context) error
}

type Remote struct {
	Job         *job.Job
	Cluster     cluster.Cluster
	Connections map[string]lrmrpb.NodeClient
	Input       input.Feeder
	Broadcast   serialization.Broadcast
	rowChanLen  int
}

func NewRemote(
	ctx context.Context,
	j *job.Job,
	c cluster.Cluster,
	in input.Feeder,
	broadcasts serialization.Broadcast,
	options ...func(*Remote),
) (Driver, error) {
	drv := &Remote{
		Job:       j,
		Cluster:   c,
		Input:     in,
		Broadcast: broadcasts,
	}
	for _, o := range options {
		o(drv)
	}

	var (
		conns = make(map[string]lrmrpb.NodeClient)
		mu    sync.Mutex
	)
	var wg errgroup.Group
	for _, host := range j.Hostnames() {
		host := host
		wg.Go(func() error {
			conn, err := c.Connect(ctx, host)
			// If the deadline is exceeded, we just not use the host.
			if errors.Is(err, context.DeadlineExceeded) {
				return nil
			} else if err != nil {
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

	drv.Connections = conns
	return drv, nil
}

func (m *Remote) RunAttached(ctx context.Context) (Result, error) {
	syncCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 1. create the job in executors
	if err := m.createJob(syncCtx); err != nil {
		return nil, errors.Wrap(err, "create job")
	}

	asyncCtx, asyncCtxCancel := context.WithCancel(ctx)

	res := &result{
		rowChan:    make(chan lrdd.Row, m.rowChanLen),
		jobManager: job.NewLocalManager(m.Job),
		cancel:     asyncCtxCancel,
	}

	res.jobManager.OnJobCompletion(func(s *job.Status) {
		for _, err := range s.Errors {
			res.addErr(err)
		}

		asyncCtxCancel()
	})

	// 2. async start jobs and connect streams
	var wg sync.WaitGroup
	for host, rpc := range m.Connections {
		wg.Add(1)

		go func(host string, rpc lrmrpb.NodeClient) {
			defer func() {
				if err := errorist.WrapPanic(recover()); err != nil {
					res.addErr(err)
				}

				wg.Done()
			}()

			req := &lrmrpb.StartJobRequest{
				JobID: m.Job.ID,
			}
			stream, err := rpc.StartJobInForeground(asyncCtx, req)
			if err != nil {
				res.addErr(errors.Wrapf(err, "call StartJobInForeground to %s", host))
				return
			}

			for asyncCtx.Err() == nil {
				msg, err := stream.Recv()
				if err != nil {
					if err == io.EOF || status.Code(err) == codes.Canceled || errors.Is(err, context.Canceled) {
						// normal end of stream
					} else if status.Code(err) == codes.DeadlineExceeded {
						res.addErr(context.DeadlineExceeded)
					} else {
						res.addErr(errors.Wrapf(err, "receive status from %s", host))
					}
					break
				}

				// received status report message from executor node
				if msg.Type == lrmrpb.JobOutput_COLLECT_DATA {
				pushLoop:
					for _, row := range msg.Data {
						select {
						case <-asyncCtx.Done():
							break pushLoop
						default:
							value := lrdd.GetValue(lrdd.RowType(msg.RowType))
							_, err := value.UnmarshalMsg(row.Value)
							if err != nil {
								res.addErr(err)
								break pushLoop
							}
							marshalUnmarshalerRow := lrdd.Row{
								Key:   row.Key,
								Value: value,
							}

							res.rowChan <- marshalUnmarshalerRow
						}
					}
				} else if msg.Type == lrmrpb.JobOutput_REPORT_TASK_COMPLETION {
					taskID := job.TaskID{
						JobID:       m.Job.ID,
						StageName:   msg.Stage,
						PartitionID: msg.PartitionID,
					}
					switch msg.TaskStatus {
					case lrmrpb.JobOutput_SUCCEED:
						_ = res.jobManager.MarkTaskAsSucceed(asyncCtx, taskID, msg.Metrics)
					case lrmrpb.JobOutput_FAILED:
						_ = res.jobManager.MarkTaskAsFailed(asyncCtx, taskID, errors.New(msg.Error), msg.Metrics)
					default:
						log.Warn().
							Str("task_id", taskID.String()).
							Str("status", msg.TaskStatus.String()).
							Msg("unexpected task status")
					}
				}
			}

			err = stream.CloseSend()
			if err != nil {
				res.addErr(errors.Wrapf(err, "close stream to %s", host))
			}
		}(host, rpc)
	}

	go func() {
		wg.Wait()

		close(res.rowChan)
	}()

	// 3. feed inputs simultaneously
	err := m.feedInput(syncCtx, m.Job, m.Input)
	if err != nil {
		return nil, err
	}

	return res, nil
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

// WithRowChanLen sets the length of the channel for receiving rows.
func WithRowChanLen(rowChanLen int) func(*Remote) {
	return func(r *Remote) {
		r.rowChanLen = rowChanLen
	}
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
