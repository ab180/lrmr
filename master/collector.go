package master

import (
	"context"
	"fmt"
	"github.com/airbloc/logger/module/loggergrpc"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/input"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"strings"
	"sync"
	"time"
)

const CollectStageName = "__collect"

type Collector struct {
	m    *Master
	srv  *grpc.Server
	Node *node.Node

	runningJobInputs sync.Map
	jobResultChans   sync.Map
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

func (c *Collector) Collect(jobID string) {
	in := input.NewReader(c.m.opt.Output.BufferLength)
	resultChan := make(chan map[string][]*lrdd.Row, 1)

	c.runningJobInputs.Store(jobID, in)
	c.jobResultChans.Store(jobID, resultChan)

	go func() {
		totalRows := 0
		results := make(map[string][]*lrdd.Row)
		for rows := range in.C {
			for _, row := range rows {
				results[row.Key] = append(results[row.Key], row)
				totalRows += 1
			}
		}
		log.Verbose("Collected {} results from {} partitions", totalRows, len(results))
		resultChan <- results

		c.runningJobInputs.Delete(jobID)
		c.jobResultChans.Delete(jobID)
	}()
}

func (c *Collector) Results(jobID string) (<-chan map[string][]*lrdd.Row, error) {
	v, ok := c.jobResultChans.Load(jobID)
	if !ok {
		return nil, errors.Errorf("job %s not found", jobID)
	}
	return v.(chan map[string][]*lrdd.Row), nil
}

func (c *Collector) PushData(stream lrmrpb.Node_PushDataServer) error {
	h, err := lrmrpb.DataHeaderFromMetadata(stream)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}

	jobID := strings.Split(h.TaskID, "/")[0]
	v, ok := c.runningJobInputs.Load(jobID)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "unknown job ID: %s", jobID)
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

func (c *Collector) CreateTask(context.Context, *lrmrpb.CreateTaskRequest) (*empty.Empty, error) {
	return &empty.Empty{}, nil
}

func (c *Collector) Stop() {
	// TODO: shutdown timeout
	c.srv.GracefulStop()
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
