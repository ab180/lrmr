package lrmr

import (
	"context"
	"fmt"
	"time"

	"github.com/goombaio/namegenerator"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/master"
	"github.com/therne/lrmr/partitions"
	"github.com/therne/lrmr/stage"
	"github.com/therne/lrmr/transformation"
)

// Dataset is less-resilient distributed dataset
type Dataset struct {
	Session   *Session
	NumStages int

	defaultPlan partitions.Plan
}

func FromInput(provider InputProvider) *Dataset {
	sess := NewSession().SetInput(provider)
	return &Dataset{Session: sess}
}

func FromURI(uri string) *Dataset {
	sess := NewSession().SetInput(&localInput{Path: uri})
	return &Dataset{Session: sess}
}

func Parallelize(input interface{}, m *master.Master) *Dataset {
	data := lrdd.From(input)
	return FromInput(&parallelizedInput{Data: data})
}

func (d *Dataset) addStage(tf transformation.Transformation) {
	d.Session.AddStage(tf)
}

func (d *Dataset) Do(t Transformer) *Dataset {
	d.addStage(&transformerTransformation{t})
	return d
}

func (d *Dataset) Map(m Mapper) *Dataset {
	d.addStage(&mapTransformation{m})
	return d
}

func (d *Dataset) FlatMap(fm FlatMapper) *Dataset {
	d.addStage(&flatMapTransformation{fm})
	return d
}

func (d *Dataset) Reduce(r Reducer) *Dataset {
	d.addStage(&reduceTransformation{r})
	return d
}

func (d *Dataset) Sort(s Sorter) *Dataset {
	d.addStage(&sortTransformation{r})
	return d
}

func (d *Dataset) GroupByKey() *Dataset {
	d.Session.SetPartitioner(partitions.NewHashKeyPartitioner())
	return d
}

func (d *Dataset) GroupByKnownKeys(knownKeys []string) *Dataset {
	d.Session.SetPartitioner(partitions.NewFiniteKeyPartitioner(knownKeys))
	return d
}

func (d *Dataset) Repartition(n int) *Dataset {
	d.Session.SetDesiredPartitionCount(n)
	return d
}

func (d *Dataset) PartitionedBy(p partitions.Partitioner) *Dataset {
	d.Session.SetPartitioner(p)
	return d
}

func (d *Dataset) Broadcast(key string, value interface{}) *Dataset {
	d.Session.Broadcast(key, value)
	return d
}

func (d *Dataset) WithWorkerCount(n int) *Dataset {
	d.Session.DefaultPlan().MaxNodes = n
	return d
}

func (d *Dataset) WithConcurrencyPerWorker(n int) *Dataset {
	d.Session.DefaultPlan().ExecutorsPerNode = n
	return d
}

func (d *Dataset) Collect(ctx context.Context, m *master.Master, jobName ...string) (map[string][]*lrdd.Row, error) {
	if len(jobName) == 0 {
		jobName = append(jobName, randomJobName())
	}
	job, err := d.Session.Run(ctx, jobName[0], m)
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
