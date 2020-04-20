package job

import (
	"context"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/job/partitions"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"golang.org/x/sync/errgroup"
)

var (
	ErrNoAvailableWorkers = errors.New("no available workers")
	ErrInsufficientPlans  = errors.New("insufficient plans")
)

type PartitionPlan struct {
	Partitioner lrmrpb.Output_PartitionerType
	PlanOptions []partitions.PlanOption
}

type Scheduler struct {
	nodeManager node.Manager
	log         logger.Logger
}

func NewScheduler(nm node.Manager) *Scheduler {
	return &Scheduler{
		nodeManager: nm,
		log:         logger.New("jobscheduler"),
	}
}

// Schedule plans partition according to given PartitionPlans and stages,
// and then create tasks to the nodes with the plan.
func (sch *Scheduler) Schedule(ctx context.Context, j *Job, plans []PartitionPlan, broadcasts map[string][]byte) (*Job, []partitions.PhysicalPlans, error) {
	if len(plans) != len(j.Stages)+1 {
		return nil, nil, ErrInsufficientPlans
	}

	workers, err := sch.nodeManager.List(ctx, node.Worker)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "list available workers")
	}
	if len(workers) == 0 {
		return nil, nil, ErrNoAvailableWorkers
	}
	sched, err := partitions.NewSchedulerWithNodes(ctx, nil, workers)
	if err != nil {
		return nil, nil, errors.WithMessage(err, "start scheduling")
	}

	physicalPlans := make([]partitions.PhysicalPlans, len(plans))
	for i, p := range plans {
		if i == len(j.Stages) {
			p.PlanOptions = []partitions.PlanOption{partitions.WithEmpty()}
		}
		logical, physical := sched.Plan(p.PlanOptions...)

		physicalPlans[i] = physical
		if i > 0 {
			j.Stages[i-1].Partitions = logical
		}
		var stageName string
		if i < len(j.Stages) {
			stageName = j.Stages[i].Name
		} else {
			stageName = "__final"
		}
		sch.log.Verbose("Planned {} partitions on {}/{}:\n{}",
			len(physical), j.Name, stageName, physical.Pretty())
	}

	// initialize tasks reversely, so that outputs can be connected with next stage
	for i := len(j.Stages) - 1; i >= 0; i-- {
		wg, reqCtx := errgroup.WithContext(ctx)
		for _, curPartition := range physicalPlans[i] {
			w := curPartition.Node
			req := &lrmrpb.CreateTaskRequest{
				JobID:      j.ID,
				StageName:  j.Stages[i].Name,
				Input:      buildInputAt(j.Stages, i, curPartition),
				Output:     buildOutputTo(plans, j.Stages, i+1, physicalPlans[i+1]),
				Broadcasts: broadcasts,
			}
			wg.Go(func() error {
				conn, err := sch.nodeManager.Connect(ctx, w.Host)
				if err != nil {
					return errors.Wrap(err, "grpc dial")
				}
				if _, err := lrmrpb.NewWorkerClient(conn).CreateTask(reqCtx, req); err != nil {
					return errors.Wrap(err, "call CreateTask")
				}
				return nil
			})
		}
		if err := wg.Wait(); err != nil {
			return nil, nil, err
		}
	}
	return j, physicalPlans, nil
}

func buildInputAt(stages []*Stage, stageIdx int, curPartition partitions.PhysicalPlan) *lrmrpb.Input {
	var prevStageName string
	if stageIdx == 0 {
		prevStageName = "__input"
	} else {
		prevStageName = stages[stageIdx-1].Name
	}
	return &lrmrpb.Input{
		Type:          lrmrpb.Input_PUSH,
		PartitionKey:  curPartition.Key,
		PrevStageName: prevStageName,
	}
}

func buildOutputTo(plans []PartitionPlan, stages []*Stage, nextStageIdx int, nextPartitions partitions.PhysicalPlans) *lrmrpb.Output {
	var nextStageName string
	if nextStageIdx < len(stages) {
		nextStageName = stages[nextStageIdx].Name
	} else {
		nextStageName = "__final"
	}
	return &lrmrpb.Output{
		Type:            lrmrpb.Output_PUSH,
		Partitioner:     plans[nextStageIdx].Partitioner,
		PartitionToHost: nextPartitions.ToMap(),
		NextStageName:   nextStageName,
	}
}
