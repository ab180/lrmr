package lrmr

import (
	"context"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/lrmrpb"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/remote"
	"github.com/vmihailenco/msgpack"
	"path"
	"reflect"
)

type Result struct {
	Ok     bool
	Errors []error
	Output interface{}
}

type Session interface {
	Broadcast(key string, val interface{})
	AddStage(executable Task) Session
	Call() (*Result, error)
	Run() (chan Result, string, error)
}

type session struct {
	ctx    context.Context
	stages []Task

	broadcasts           map[string]interface{}
	serializedBroadcasts map[string][]byte

	nodeManager node.Manager
	masterNode  *remote.Master

	log logger.Logger
	opt *Options
}

func (s *session) Broadcast(key string, val interface{}) {
	sv, err := msgpack.Marshal(val)
	if err != nil {
		panic("broadcast value must be serializable: " + err.Error())
	}
	s.serializedBroadcasts[key] = sv
	s.broadcasts[key] = val
}

func (s *session) AddStage(task Task) Session {
	s.stages = append(s.stages, task)
}

func (s *session) plan() (reqs []*lrmrpb.PrepareRequest, err error) {
	planCtx := planContext{s}

	assignments := make([][]*node.JobAssignment, len(s.stages))
	for _, task := range s.stages {
		taskTyp := reflect.TypeOf(task)
		taskName := path.Join(taskTyp.PkgPath(), taskTyp.Name())

		plans, err := task.Plan(&planCtx)
		if err != nil {
			err = fmt.Errorf("plan %s: %w", taskName, err)
			return
		}

		var jobs []*node.Job
		for _, plan := range plans {
			jobs = append(jobs, &node.Job{
				Task:         taskName,
				Key:          plan.Key,
				Complexity:   plan.Complexity,
				AffinityRule: make(map[string]string),
			})
		}
		scheduleResult, err := s.nodeManager.ScheduleJobs(s.ctx, jobs)
		if err != nil {
			err = fmt.Errorf("schedule %s: %w", taskName, err)
			return
		}
		reqs = append(reqs, &lrmrpb.PrepareRequest{
			Current: &lrmrpb.Stage{
				TaskName: taskName,
				JobID:    scheduleResult.JobID,
			},
			Inputs:     scheduleResult.ScheduledNodes,
			Broadcasts: s.serializedBroadcasts,
		})
		assignments = append(assignments, scheduleResult.Assignments)
	}
	// fill output and next item.
	// leave final stage's output empty so Call() and Run() can fill it
	for i := 0; i < len(reqs)-1; i++ {
		outputs := map[string]string{}
		for _, assignment := range assignments[i+1] {
			outputs[assignment.Job.Key] = assignment.Node.Host
		}
		reqs[i].Outputs = outputs
		reqs[i].Next = reqs[i+1].Current
	}
	return
}

func (s *session) Call() (*Result, error) {
	reqs, err := s.plan()
	if err != nil {
		return nil, err
	}
	jobID := reqs[0].Current.JobID

	// connect last stage output to master node (self)
	reqs[len(reqs)-1].Next = &lrmrpb.Stage{
		TaskName: "__result",
		JobID:    jobID,
	}
	reqs[len(reqs)-1].Outputs = map[string]string{
		"*": s.nodeManager.Self().Host,
	}

	// TODO: temporary result format
	results := <-s.masterNode.SubscribeResult(jobID)
	return &Result{
		Ok:     results[0]["ok"] == true,
		Errors: nil,
		Output: results[0]["output"],
	}, nil
}

func (s *session) Run() (chan *Result, string, error) {
	reqs, err := s.plan()
	if err != nil {
		return nil, "", err
	}
	jobID := reqs[0].Current.JobID

	// connect last stage output to master node (self)
	reqs[len(reqs)-1].Next = &lrmrpb.Stage{
		TaskName: "__result",
		JobID:    jobID,
	}
	reqs[len(reqs)-1].Outputs = map[string]string{
		"*": s.nodeManager.Self().Host,
	}

	asyncResultChan := make(chan *Result)
	go func() {
		results := <-s.masterNode.SubscribeResult(jobID)
		asyncResultChan <- &Result{
			Ok:     results[0]["ok"] == true,
			Errors: nil,
			Output: results[0]["output"],
		}
	}()
	return asyncResultChan, jobID, nil
}

type planContext struct {
	session *session
}

func (p *planContext) Broadcast(key string) interface{} {
	return p.session.broadcasts[key]
}
