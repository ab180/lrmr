package lrmr

import (
	"fmt"

	"github.com/ab180/lrmr/internal/util"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
	"github.com/ab180/lrmr/master"
	"github.com/ab180/lrmr/partitions"
	"github.com/ab180/lrmr/stage"
	"github.com/ab180/lrmr/transformation"
)

// Dataset is less-resilient distributed dataset
type Dataset struct {
	session *Session

	input  InputProvider
	stages []stage.Stage

	// len(plans) == len(stages)+1 (because of input stage)
	plans       []partitions.Plan
	defaultPlan partitions.Plan

	NumStages int
}

func newDataset(sess *Session, input InputProvider) *Dataset {
	return &Dataset{
		session: sess,
		input:   input,
		stages:  []stage.Stage{{Name: "_input"}},
		plans: []partitions.Plan{
			{Partitioner: input, DesiredCount: 1, MaxNodes: 1, DesiredNodeAffinity: map[string]string{"Type": "master"}},
		},
	}
}

func (d *Dataset) addStage(name string, tf transformation.Transformation) {
	st := stage.New(name, tf, stage.InputFrom(*d.lastStage()))
	d.lastStage().SetOutputTo(st)

	d.stages = append(d.stages, st)
	d.plans = append(d.plans, d.defaultPlan)
}

func (d *Dataset) Do(t Transformer) *Dataset {
	d.addStage(d.stageName(t), &transformerTransformation{t})
	return d
}

func (d *Dataset) Map(m Mapper) *Dataset {
	d.addStage(d.stageName(m), &mapTransformation{m})
	return d
}

func (d *Dataset) FlatMap(fm FlatMapper) *Dataset {
	d.addStage(d.stageName(fm), &flatMapTransformation{fm})
	return d
}

func (d *Dataset) Reduce(r Reducer) *Dataset {
	d.addStage(d.stageName(r), &reduceTransformation{r})
	return d
}

func (d *Dataset) Sort(s Sorter) *Dataset {
	d.addStage(d.stageName(s), &sortTransformation{sorter: s})
	return d
}

func (d *Dataset) GroupByKey() *Dataset {
	d.lastPlan().Partitioner = partitions.NewHashKeyPartitioner()
	return d
}

func (d *Dataset) GroupByKnownKeys(knownKeys []string) *Dataset {
	d.lastPlan().Partitioner = partitions.NewFiniteKeyPartitioner(knownKeys)
	return d
}

func (d *Dataset) Shuffle() *Dataset {
	d.lastPlan().Partitioner = partitions.NewShuffledPartitioner()
	return d
}

func (d *Dataset) Repartition(n int) *Dataset {
	d.defaultPlan.DesiredCount = n
	return d
}

func (d *Dataset) PartitionedBy(p partitions.Partitioner) *Dataset {
	d.plans[len(d.plans)-1].Partitioner = p
	return d
}

func (d *Dataset) Broadcast(key string, value interface{}) *Dataset {
	d.session.Broadcast(key, value)
	return d
}

func (d *Dataset) WithWorkerCount(n int) *Dataset {
	d.defaultPlan.MaxNodes = n
	return d
}

func (d *Dataset) WithConcurrencyPerWorker(n int) *Dataset {
	d.defaultPlan.ExecutorsPerNode = n
	return d
}

func (d *Dataset) Collect() ([]*lrdd.Row, error) {
	// add collect stage for the master
	d.PartitionedBy(master.NewCollectPartitioner()).
		Repartition(1).
		WithWorkerCount(1).
		WithConcurrencyPerWorker(1).
		addStage(master.CollectStageName, &master.Collector{})

	j, err := d.session.Run(d)
	if err != nil {
		return nil, err
	}
	res, err := j.Collect()
	if err != nil {
		if jobErr, ok := err.(*job.Error); ok {
			log.Error("Job failed. Cause: {}", jobErr.Message)
			log.Error("  (caused by task {})", jobErr.Task)
		}
		return nil, err
	}
	log.Info("Successfully collected {} results for job {}.", len(res), j.ID)
	return res, nil
}

func (d *Dataset) stageName(v interface{}) string {
	name := fmt.Sprintf("%s%d", util.NameOfType(v), d.NumStages)
	d.NumStages += 1
	return name
}

func (d *Dataset) Run() (*RunningJob, error) {
	return d.session.Run(d)
}

func (d *Dataset) lastStage() *stage.Stage {
	return &d.stages[len(d.stages)-1]
}

func (d *Dataset) lastPlan() *partitions.Plan {
	return &d.plans[len(d.plans)-1]
}
