package lrmr

import (
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/worker"
)

type Option func(op *Options)

type Options struct {
	OutputChannelSize int `json:"outputBufferSize"`
	OutputBufferSize  int `json:"outputBufferSize"`

	Host          string
	EtcdEndpoints []string

	NodeManager *node.ManagerOptions
	Worker      *worker.Options
}

func DefaultOptions() *Options {
	return &Options{
		OutputChannelSize: 1000,
		OutputBufferSize:  1000,
		Host:              "localhost",
		EtcdEndpoints:     []string{"127.0.0.1:2379"},
		NodeManager:       node.DefaultManagerOptions(),
		Worker:            worker.DefaultOptions(),
	}
}
