package lrmr

import (
	"fmt"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/stage"
)

// Dataset is less-resilient distributed dataset
type Dataset struct {
	Session
	NumStages int
}

func Input(provider InputProvider, m *Master) *Dataset {
	sess := NewSession(m).SetInput(provider)
	return &Dataset{Session: sess}
}

func TextFile(uri string, m *Master) *Dataset {
	sess := NewSession(m).SetInput(&localInput{Path: uri})
	return &Dataset{Session: sess}
}

func (d *Dataset) FlatMap(mapper stage.FlatMapper) *Dataset {
	d.Session.AddStage(stage.LookupByRunner(mapper), mapper)
	return d
}

func (d *Dataset) Reduce(reducer stage.Reducer) *Dataset {
	d.Session.AddStage(stage.LookupByRunner(reducer), reducer)
	return d
}

func (d *Dataset) GroupByKey() *Dataset {
	d.Session.Output(job.DescribingStageOutput().WithPartitions())
	return d
}

func (d *Dataset) GroupByKnownKeys(knownKeys []string) *Dataset {
	d.Session.Output(job.DescribingStageOutput().WithFixedPartitions(knownKeys))
	return d
}

func (d *Dataset) NoOutput() *Dataset {
	d.Session.Output(job.DescribingStageOutput().Nothing())
	return d
}

func (d *Dataset) stageName(s stage.Stage) string {
	name := fmt.Sprintf("%s%d", s.Name, d.NumStages)
	d.NumStages += 1
	return name
}
