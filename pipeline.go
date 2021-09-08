package lrmr

import (
	"context"
	"fmt"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/driver"
	"github.com/ab180/lrmr/executor"
	"github.com/ab180/lrmr/input"
	"github.com/ab180/lrmr/internal/serialization"
	"github.com/ab180/lrmr/internal/util"
	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/job/stage"
	"github.com/ab180/lrmr/partitions"
	"github.com/ab180/lrmr/transformation"
	"github.com/pkg/errors"
)

// ErrNoAvailableExecutors is raised when any executor nodes are found on the cluster trying to run a job.
var ErrNoAvailableExecutors = errors.New("no available executors in cluster")

type Pipeline struct {
	input  input.Feeder
	stages []*stage.Stage

	// len(plans) == len(stages)+1 (because of input stage)
	plans         []partitions.Plan
	nextStagePlan partitions.Plan

	broadcasts serialization.Broadcast
	options    PipelineOptions
}

func NewPipeline(in input.Feeder, opts ...PipelineOption) *Pipeline {
	return &Pipeline{
		input: in,
		stages: []*stage.Stage{
			{Name: "_input"},
		},
		plans: []partitions.Plan{
			{DesiredCount: partitions.None},
		},
		nextStagePlan: partitions.Plan{},
		broadcasts:    make(serialization.Broadcast),
		options:       buildSessionOptions(opts),
	}
}

// Broadcast shares given value across the cluster. The data broadcast this way
// is cached in serialized form and deserialized before running each task.
func (p *Pipeline) Broadcast(key string, val interface{}) *Pipeline {
	p.broadcasts[key] = val
	return p
}

// AddStage adds an transformation.Transformation to the pipeline.
func (p *Pipeline) AddStage(tf transformation.Transformation) *Pipeline {
	stageName := util.NameOfType(tf)
	lastStage := p.stages[len(p.stages)-1]

	newStage := stage.New(fmt.Sprintf("%s%d", stageName, len(p.stages)), tf, stage.InputFrom(lastStage))
	lastStage.SetOutputTo(newStage)

	p.stages = append(p.stages, newStage)
	p.plans = append(p.plans, p.nextStagePlan)
	return p
}

func (p *Pipeline) Do(t Transformer) *Pipeline {
	return p.AddStage(&transformerTransformation{t})
}

func (p *Pipeline) Map(m Mapper) *Pipeline {
	return p.AddStage(&mapTransformation{m})
}

func (p *Pipeline) FlatMap(fm FlatMapper) *Pipeline {
	return p.AddStage(&flatMapTransformation{fm})
}

func (p *Pipeline) Reduce(r Reducer) *Pipeline {
	return p.AddStage(&reduceTransformation{r})
}

func (p *Pipeline) Sort(s Sorter) *Pipeline {
	return p.AddStage(&sortTransformation{sorter: s})
}

func (p *Pipeline) GroupByKey() *Pipeline {
	p.plans[len(p.plans)-1].Partitioner = partitions.NewHashKeyPartitioner()
	return p
}

func (p *Pipeline) GroupByKnownKeys(knownKeys []string) *Pipeline {
	p.plans[len(p.plans)-1].Partitioner = partitions.NewFiniteKeyPartitioner(knownKeys)
	return p
}

func (p *Pipeline) Shuffle() *Pipeline {
	p.plans[len(p.plans)-1].Partitioner = partitions.NewShuffledPartitioner()
	return p
}

func (p *Pipeline) Repartition(n int) *Pipeline {
	p.nextStagePlan.DesiredCount = n
	return p
}

func (p *Pipeline) PartitionedBy(partitioner partitions.Partitioner) *Pipeline {
	p.plans[len(p.plans)-1].Partitioner = partitioner
	return p
}

func (p *Pipeline) WithWorkerCount(n int) *Pipeline {
	p.nextStagePlan.MaxNodes = n
	return p
}

func (p *Pipeline) WithConcurrencyPerWorker(n int) *Pipeline {
	p.nextStagePlan.ExecutorsPerNode = n
	return p
}

func (p *Pipeline) createJob(ctx context.Context, c cluster.Cluster) (*job.Job, error) {
	ctx, cancel := context.WithTimeout(ctx, p.options.StartTimeout)
	defer cancel()

	jobID := p.options.Name
	if jobID == "" {
		jobID = util.GenerateID("J")
	}
	executors, err := p.listExecutors(ctx, c)
	if err != nil {
		return nil, err
	}
	return job.Create(jobID, p.stages, executors, p.plans), nil
}

func (p *Pipeline) listExecutors(ctx context.Context, c cluster.Cluster) ([]*node.Node, error) {
	listOpts := cluster.ListOption{Type: node.Worker}
	if p.options.NodeSelector != nil {
		listOpts.Tag = p.options.NodeSelector
	}
	executors, err := c.List(ctx, listOpts)
	if err != nil {
		return nil, errors.WithMessage(err, "list available executors")
	}
	if len(executors) == 0 {
		return nil, ErrNoAvailableExecutors
	}
	return executors, nil
}

func (p *Pipeline) RunInBackground(c cluster.Cluster) (*RunningJob, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.options.StartTimeout)
	defer cancel()

	j, err := p.createJob(ctx, c)
	if err != nil {
		return nil, errors.Wrap(err, "create job")
	}
	drv, err := driver.NewRemote(ctx, j, c, p.input, p.broadcasts)
	if err != nil {
		return nil, errors.Wrap(err, "initiate job driver")
	}
	runningJob := startTrackingDetachedJob(j, c.States(), drv)
	if err := drv.RunDetached(ctx); err != nil {
		return nil, err
	}
	return runningJob, nil
}

func (p *Pipeline) RunAndCollect(ctx context.Context, c cluster.Cluster) (*driver.CollectResult, error) {
	j, err := p.createJob(ctx, c)
	if err != nil {
		return nil, errors.Wrap(err, "create job")
	}
	j.Stages[len(j.Stages)-1].Output = stage.Output{
		Stage:       executor.CollectStageName,
		Partitioner: partitions.WrapPartitioner(partitions.NewPreservePartitioner()),
	}
	drv, err := driver.NewRemote(context.Background(), j, c, p.input, p.broadcasts)
	if err != nil {
		return nil, errors.Wrap(err, "initiate job driver")
	}
	return drv.RunAttached(ctx)
}
