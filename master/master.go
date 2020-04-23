package master

import (
	"github.com/airbloc/logger"
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/node"
)

var log = logger.New("master")

type Master struct {
	node *node.Node

	JobManager   job.Manager
	JobTracker   *job.Tracker
	JobReporter  *job.Reporter
	JobScheduler *job.Scheduler
	NodeManager  node.Manager

	opt Options
}

func New(crd coordinator.Coordinator, opt Options) (*Master, error) {
	nm, err := node.NewManager(crd, opt.RPC)
	if err != nil {
		return nil, err
	}
	return &Master{
		node: &node.Node{
			ID:   "master",
			Host: opt.AdvertisedHost,
		},
		JobManager:   job.NewManager(nm, crd),
		JobTracker:   job.NewJobTracker(crd),
		JobReporter:  job.NewJobReporter(crd),
		JobScheduler: job.NewScheduler(nm),
		NodeManager:  nm,
		opt:          opt,
	}, nil
}

func (m *Master) Start() {
	go m.JobTracker.HandleJobCompletion()
}

func (m *Master) Stop() {
	m.JobTracker.Close()
	if err := m.NodeManager.Close(); err != nil {
		log.Error("failed to close node manager", err)
	}
}
