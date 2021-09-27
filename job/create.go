package job

import (
	"fmt"
	"time"

	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/job/stage"
	"github.com/ab180/lrmr/partitions"
)

// Create creates a new job.
// It schedules tasks (partitions) with given plan and assigns them over the nodes.
func Create(id string, stages []*stage.Stage, nodes []*node.Node, plans []partitions.Plan) *Job {
	pp, assignments := partitions.Schedule(nodes, plans)
	for i, p := range pp {
		stages[i].Output.Partitioner = p.Partitioner

		partitionerName := fmt.Sprintf("%T", partitions.UnwrapPartitioner(p.Partitioner))
		log.Verbose("Planned {} partitions on {}/{} (output with {}):\n{}", len(p.Partitions),
			id, stages[i].Name, partitionerName, assignments[i].Pretty())
	}

	return &Job{
		ID:          id,
		Stages:      stages,
		Partitions:  assignments,
		SubmittedAt: time.Now(),
	}
}
