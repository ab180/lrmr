package lrmr

import (
	"fmt"
	"github.com/therne/lrmr/job/partitions"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/stage"
)

// Dataset is less-resilient distributed dataset
type Dataset struct {
	Session
	NumStages int

	defaultPartitionOpts []partitions.PlanOption
}

func FromInput(provider InputProvider, m *Master) *Dataset {
	sess := NewSession(m).SetInput(provider)
	return &Dataset{Session: sess}
}

func FromURI(uri string, m *Master) *Dataset {
	sess := NewSession(m).SetInput(&localInput{Path: uri})
	return &Dataset{Session: sess}
}

func (d *Dataset) addStage(runner interface{}) {
	d.Session.AddStage(stage.LookupByRunner(runner), runner)
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
	d.Session.SetPartitionType(lrmrpb.Output_FINITE_KEY)
	d.Session.SetPartitionOption(partitions.WithFixedKeys(knownKeys))
	return d
}

func (d *Dataset) Repartition(n int) *Dataset {
	d.Session.SetPartitionOption(partitions.WithFixedCount(n))
	return d
}

func (d *Dataset) Broadcast(key string, value interface{}) *Dataset {
	d.Session.Broadcast(key, value)
	return d
}

func (d *Dataset) WithWorkerCount(n int) *Dataset {
	d.defaultPartitionOpts = append(d.defaultPartitionOpts, partitions.WithLimitNodes(n))
	return d
}

func (d *Dataset) WithConcurrencyPerWorker(n int) *Dataset {
	d.defaultPartitionOpts = append(d.defaultPartitionOpts, partitions.WithExecutorsPerNode(n))
	return d
}

func (d *Dataset) stageName(s stage.Stage) string {
	name := fmt.Sprintf("%s%d", s.Name, d.NumStages)
	d.NumStages += 1
	return name
}
