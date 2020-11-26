package test

import (
	"github.com/ab180/lrmr"
	"github.com/ab180/lrmr/lrdd"
	partitions "github.com/ab180/lrmr/partitions"
)

var _ = lrmr.RegisterTypes(&tagNodeType{})

func AssignTaskOnMaster(sess *lrmr.Session) *lrmr.Dataset {
	in := make([]string, 0)
	return sess.Parallelize([]*lrdd.Row{lrdd.Value(in)}).
		Map(&tagNodeType{}).
		PartitionedBy(partitions.WithAssignmentToMaster(partitions.NewShuffledPartitioner())).
		Map(&tagNodeType{})
}

type tagNodeType struct{}

func (d *tagNodeType) Map(ctx lrmr.Context, row *lrdd.Row) (*lrdd.Row, error) {
	var tags []string
	row.UnmarshalValue(&tags)

	isWorker := ctx.WorkerLocalOption("IsWorker")
	if isWorker == nil {
		tags = append(tags, "master")
	} else {
		tags = append(tags, "worker")
	}
	return lrdd.Value(tags), nil
}
