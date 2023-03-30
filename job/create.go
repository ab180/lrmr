package job

import (
	"fmt"
	"time"

	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/job/stage"
	"github.com/ab180/lrmr/partitions"
	"github.com/rs/zerolog/log"
)

// Create creates a new job.
// It schedules tasks (partitions) with given plan and assigns them over the nodes.
func Create(id string, stages []*stage.Stage, nodes []*node.Node, plans []partitions.Plan) *Job {
	pp, assignments := partitions.Schedule(nodes, plans)
	for i, p := range pp {
		stages[i].Output.Partitioner = p.Partitioner

		partitionerName := fmt.Sprintf("%T", partitions.UnwrapPartitioner(p.Partitioner))
		log.Debug().
			Str("job_id", id).
			Str("stageName", stages[i].Name).
			Int("partitions", len(p.Partitions)).
			Str("partitioner", partitionerName).
			Str("assignments", assignments[i].Pretty()).
			Msg("planned")
	}

	return &Job{
		ID:          id,
		Stages:      stages,
		Partitions:  assignments,
		SubmittedAt: time.Now(),
	}
}
