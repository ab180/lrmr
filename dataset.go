package lrmr

import (
	"context"
	"fmt"
	"github.com/goombaio/namegenerator"
	"github.com/therne/lrmr/job/partitions"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/stage"
	"time"
)

// Dataset is less-resilient distributed dataset
type Dataset struct {
	Session
	NumStages int

	defaultPartitionOpts []partitions.PlanOption
}

func FromInput(provider InputProvider, m *master.Master) *Dataset {
	sess := NewSession(m).SetInput(provider)
	return &Dataset{Session: sess}
}

func FromURI(uri string, m *master.Master) *Dataset {
	sess := NewSession(m).SetInput(&localInput{Path: uri})
	return &Dataset{Session: sess}
}

func (d *Dataset) addStage(runner interface{}) {
	d.Session.AddStage(runner)
	d.Session.SetPartitionOption(d.defaultPartitionOpts...)
}

func (d *Dataset) Do(runner stage.Runner) *Dataset {
	d.addStage(runner)
	return d
}

func (d *Dataset) FlatMap(mapper stage.FlatMapper) *Dataset {
	d.addStage(mapper)
	return d
}

func (d *Dataset) Reduce(reducer stage.Reducer) *Dataset {
	d.addStage(reducer)
	return d
}

func (d *Dataset) Sort(sorter stage.Sorter) *Dataset {
	d.addStage(sorter)
	return d
}

func (d *Dataset) GroupByKey() *Dataset {
	d.Session.SetPartitionType(lrmrpb.Output_HASH_KEY)
	return d
}

func (d *Dataset) GroupByKnownKeys(knownKeys []string) *Dataset {
	opts := append(d.defaultPartitionOpts, partitions.WithFixedKeys(knownKeys))
	d.Session.SetPartitionType(lrmrpb.Output_FINITE_KEY)
	d.Session.SetPartitionOption(opts...)
	return d
}

func (d *Dataset) Repartition(n int) *Dataset {
	opts := append(d.defaultPartitionOpts, partitions.WithFixedCount(n))
	d.Session.SetPartitionOption(opts...)
	return d
}

func (d *Dataset) PartitionedBy(planner partitions.LogicalPlanner) *Dataset {
	opts := append(d.defaultPartitionOpts, partitions.WithLogicalPlanner(planner))
	d.Session.SetPartitionOption(opts...)
	return d
}

func (d *Dataset) Broadcast(key string, value interface{}) *Dataset {
	d.Session.Broadcast(key, value)
	return d
}

func (d *Dataset) WithWorkerCount(n int) *Dataset {
	d.defaultPartitionOpts = append(d.defaultPartitionOpts, partitions.WithLimitNodes(n))
	d.SetPartitionOption(d.defaultPartitionOpts...)
	return d
}

func (d *Dataset) WithConcurrencyPerWorker(n int) *Dataset {
	d.defaultPartitionOpts = append(d.defaultPartitionOpts, partitions.WithExecutorsPerNode(n))
	d.SetPartitionOption(d.defaultPartitionOpts...)
	return d
}

func (d *Dataset) Collect(ctx context.Context, jobName ...string) (map[string][]*lrdd.Row, error) {
	if len(jobName) == 0 {
		jobName = append(jobName, randomJobName())
	}
	job, err := d.Session.Run(ctx, jobName[0])
	if err != nil {
		return nil, err
	}
	return job.Collect()
}

func (d *Dataset) stageName(s stage.Stage) string {
	name := fmt.Sprintf("%s%d", s.Name, d.NumStages)
	d.NumStages += 1
	return name
}

func randomJobName() string {
	return namegenerator.NewNameGenerator(time.Now().UnixNano()).Generate()
}
