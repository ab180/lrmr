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

var ErrInsufficientPlans = errors.New("insufficient plans")

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

// AssignTasks create tasks to the nodes with the plan.
func (sch *Scheduler) AssignTasks(ctx context.Context, j *Job, plans []PartitionPlan, physicalPlans []partitions.PhysicalPlans, broadcasts map[string][]byte) error {
	if len(plans) != len(j.Stages)+1 {
		return ErrInsufficientPlans
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
				if _, err := lrmrpb.NewNodeClient(conn).CreateTask(reqCtx, req); err != nil {
					return errors.Wrap(err, "call CreateTask")
				}
				return nil
			})
		}
		if err := wg.Wait(); err != nil {
			return err
		}
	}
	return nil
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
		nextStageName = "__collect"
	}
	return &lrmrpb.Output{
		Type:            lrmrpb.Output_PUSH,
		Partitioner:     plans[nextStageIdx].Partitioner,
		PartitionToHost: nextPartitions.ToMap(),
		NextStageName:   nextStageName,
	}
}
