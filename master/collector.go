package master

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/airbloc/logger/module/loggergrpc"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/input"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/partitions"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const CollectStageName = "_collect"

type CollectedResult struct {
	Result []*lrdd.Row
	Error  *job.Error
}

type Collector struct {
	m    *Master
	srv  *grpc.Server
	Node *node.Node

	resultPipes sync.Map
}

func NewCollector(m *Master) *Collector {
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(loggergrpc.UnaryServerRecover()),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
			streamErrorLogger,
			loggergrpc.StreamServerRecover(),
		)),
	)
	return &Collector{
		m:   m,
		srv: srv,
	}
}

func (c *Collector) Start() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lrmrpb.RegisterNodeServer(c.srv, c)
	lis, err := net.Listen("tcp", c.m.opt.ListenHost)
	if err != nil {
		return err
	}
	advHost := c.m.opt.AdvertisedHost
	if strings.HasSuffix(advHost, ":") {
		// port is assigned automatically
		addrFrags := strings.Split(lis.Addr().String(), ":")
		advHost += addrFrags[len(addrFrags)-1]
	}

	n := node.New(advHost, node.Master)
	if err := c.m.NodeManager.RegisterSelf(ctx, n); err != nil {
		return fmt.Errorf("register master: %w", err)
	}
	c.Node = n
	go func() {
		if err := c.srv.Serve(lis); err != nil {
			log.Error("Error on serving collector gRPC", err)
		}
	}()
	return nil
}

func (c *Collector) Prepare(jobID string) {
	pipe := input.NewReader(c.m.opt.Output.BufferLength)
	c.resultPipes.Store(jobID, pipe)
}

func (c *Collector) Collect(jobID string) <-chan CollectedResult {
	p, ok := c.resultPipes.Load(jobID)
	if !ok {
		panic("you need to call Prepare() on job " + jobID)
	}
	pipe := p.(*input.Reader)

	resultChan := make(chan CollectedResult, 1)
	go func() {
		defer close(resultChan)
		defer c.resultPipes.Delete(jobID)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		jobErrors := c.m.JobManager.WatchJobErrors(ctx, jobID)

		var res CollectedResult
	CollectingLoop:
		for {
			select {
			case rows, ok := <-pipe.C:
				if !ok {
					break CollectingLoop
				}
				res.Result = append(res.Result, rows...)

			case e := <-jobErrors:
				res.Error = &e
				break CollectingLoop
			}
		}
		if res.Error != nil {
			log.Error("Job failed. Cause: {}", res.Error.Message)
			log.Error("  (caused by task {})", res.Error.Task)
		} else {
			log.Verbose("Successfully collected {} results.", len(res.Result))
		}
		resultChan <- res
	}()
	return resultChan
}

// PushData implements lrmrpb.Node for receiving and gathering final stage's outputs.
func (c *Collector) PushData(stream lrmrpb.Node_PushDataServer) error {
	h, err := lrmrpb.DataHeaderFromMetadata(stream)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	jobID := strings.Split(h.TaskID, "/")[0]
	v, ok := c.resultPipes.Load(jobID)
	if !ok {
		return nil
	}
	in := input.NewPushStream(v.(*input.Reader), stream)
	if err := in.Dispatch(stream.Context()); err != nil {
		return errors.Wrap(err, "stream dispatch")
	}
	return nil
}

func (c *Collector) PollData(stream lrmrpb.Node_PollDataServer) error {
	return status.Error(codes.Unimplemented, "unimplemented")
}

func (c *Collector) CreateTasks(context.Context, *lrmrpb.CreateTasksRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (c *Collector) Stop() {
	// TODO: shutdown timeout
	c.srv.GracefulStop()
}

type CollectPartitioner struct{}

func NewCollectPartitioner() partitions.Partitioner {
	return &CollectPartitioner{}
}

// PlanNext assigns partition to master.
func (c CollectPartitioner) PlanNext(numExecutors int) []partitions.Partition {
	assignToMaster := map[string]string{
		"Type": string(node.Master),
	}
	return []partitions.Partition{
		{ID: "_collect", AssignmentAffinity: assignToMaster},
	}
}

// DeterminePartition always partitions data to "_collect" partition.
func (c CollectPartitioner) DeterminePartition(partitions.Context, *lrdd.Row, int) (id string, err error) {
	return "_collect", nil
}

func streamErrorLogger(srv interface{}, ss grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	// dump header on stream failure
	if err := next(srv, ss); err != nil {
		if h, herr := lrmrpb.DataHeaderFromMetadata(ss); herr == nil {
			log.Error("Collecting task {} from {} failed: {}", h.TaskID, h.FromHost, errors.Cause(err))
		}
		return err
	}
	return nil
}
