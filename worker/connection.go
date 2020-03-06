package worker

import (
	"fmt"
	"github.com/airbloc/logger"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/therne/lrmr/internal/logutils"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"sync"
)

type Connection struct {
	ctx         *taskContext
	jobReporter *node.JobReporter

	stream     lrmrpb.Worker_RunTaskServer
	fromHost   string
	fromNodeID string

	msgCount int
	wg       sync.WaitGroup

	log logger.Logger
}

func newConnection(s lrmrpb.Worker_RunTaskServer, reporter *node.JobReporter) *Connection {
	conn := &Connection{
		jobReporter: reporter,
		stream:      s,
		fromHost:    "(not known yet)",
		fromNodeID:  "-",
		log:         log,
	}
	return conn
}

func (c *Connection) SetContext(ctx *taskContext, from *lrmrpb.Node) {
	c.ctx = ctx
	c.fromHost = from.Host
	c.fromNodeID = from.ID
	c.log = c.log.WithAttrs(logger.Attrs{
		"task": ctx.task.Reference(),
		"from": from.Host,
	})
	go func() {
		// aborts when executor error occurs
		for err := range ctx.executors.Errors {
			// todo: report it only once
			_ = c.AbortTask(err)
		}
	}()
}

// Enqueue runs task asynchronously with load balancing by given concurrency.
func (c *Connection) Enqueue(data lrdd.Row) {
	if c.ctx.executors.ctx.Err() != nil {
		// executor is already terminated
		return
	}
	slot := c.msgCount % c.ctx.executors.Concurrency
	c.wg.Add(1)
	c.msgCount += 1
	c.ctx.executors.Enqueue(slot, data, c)
}

func (c *Connection) WaitForCompletion() {
	c.wg.Wait()
}

func (c *Connection) FinishTask() error {
	if err := c.ctx.executors.Close(); err != nil {
		return c.AbortTask(fmt.Errorf("flush output: %v", err))
	}
	if err := c.ctx.transformation.Teardown(c.ctx); err != nil {
		return c.AbortTask(fmt.Errorf("teardown: %v", err))
	}
	go func() {
		if err := c.ctx.shards.Close(); err != nil {
			log.Error("error closing output", err)
		}
	}()

	if err := c.jobReporter.ReportSuccess(c.ctx.task.Reference()); err != nil {
		log.Error("Task {} have been successfully done, but failed to report: {}", c.ctx.task.Reference(), err)
		return status.Errorf(codes.Internal, "task success report failed: %w", err)
	}
	return c.stream.SendAndClose(&empty.Empty{})
}
func (c *Connection) AbortTask(err error) error {
	c.ctx.executors.Cancel()

	reportErr := c.jobReporter.ReportFailure(c.ctx.task.Reference(), err)
	if reportErr != nil {
		log.Error("Task {} failed with error: {}", c.ctx.task.Reference().String(), err)
		log.Error("While reporting the error, another error occurred", err)
		return status.Errorf(codes.Internal, "task failure report failed: %v", reportErr)
	}
	log.Error("  Caused by connection from {} ({})", c.fromHost, c.fromNodeID)
	return c.stream.SendAndClose(&empty.Empty{})
}

func (c *Connection) TryRecover() {
	if err := logutils.WrapRecover(recover()); err != nil {
		if c != nil {
			_ = c.AbortTask(err)
		} else {
			log.Error("failed to warm up task. {}", err.Pretty())
		}
	}
}
