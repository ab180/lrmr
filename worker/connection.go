package worker

import (
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"sync"
)

type Connection struct {
	ctx *taskContext

	stream     lrmrpb.Worker_RunTaskServer
	fromHost   string
	fromNodeID string

	Errors   chan error
	msgCount int
	wg       sync.WaitGroup

	log logger.Logger
}

func newConnection(from *lrmrpb.Node, s lrmrpb.Worker_RunTaskServer, c *taskContext) *Connection {
	return &Connection{
		ctx:        c,
		stream:     s,
		fromHost:   from.Host,
		fromNodeID: from.ID,
		Errors:     make(chan error),
		log: log.WithAttrs(logger.Attrs{
			"task": c.task.Reference(),
			"from": from.Host,
		}),
	}
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
	c.ctx.executors.inputChans[slot] <- incomingData{
		sender: c,
		data:   data,
	}
}

func (c *Connection) WaitForCompletion() {
	c.wg.Wait()
}
