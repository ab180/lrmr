package node

import (
	"github.com/therne/lrmr/internal/utils"
	"runtime"
)

type Type string

const (
	Master Type = "master"
	Worker Type = "worker"
)

type Node struct {
	ID   string `json:"id"`
	Host string `json:"host"`
	Type Type   `json:"type"`

	Executors int `json:"executors"`

	// Tag is used for affinity rules (e.g. resource locality, ...)
	Tag map[string]string `json:"tag,omitempty"`

	// ResourceUtilization is filled by Manager.calculateResourceUtilization
	ResourceUtilization uint64 `json:"-"`
}

func New(host string) *Node {
	return &Node{
		ID:        utils.GenerateID("N"),
		Host:      host,
		Executors: runtime.NumCPU() / 2,
	}
}
