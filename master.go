package lrmr

import (
	"github.com/therne/lrmr/coordinator"
	"github.com/therne/lrmr/job"
	"github.com/therne/lrmr/node"
)

type Master struct {
	node *node.Node

	jobTracker  *job.Tracker
	jobManager  job.Manager
	nodeManager node.Manager

	opt *Options
}

func NewMaster(crd coordinator.Coordinator, opt *Options) (*Master, error) {
	nm, err := node.NewManager(crd, opt.NodeManager)
	if err != nil {
		return nil, err
	}
	return &Master{
		node: &node.Node{
			ID:   "master",
			Host: opt.Master.AdvertisedHost,
		},
		jobTracker:  job.NewJobTracker(crd),
		jobManager:  job.NewManager(nm, crd),
		nodeManager: nm,
		opt:         opt,
	}, nil
}

func (m *Master) Start() {
	m.jobTracker.Start()
}

func (m *Master) Stop() {
	m.jobTracker.Close()
}
