package node

import (
	"github.com/therne/lrmr/internal/util"
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
}

func New(host string, typ Type) *Node {
	return &Node{
		ID:        util.GenerateID("N"),
		Host:      host,
		Type:      typ,
		Executors: runtime.NumCPU(),
	}
}

func (n *Node) TagMatches(selector map[string]string) bool {
	for k, v := range selector {
		if n.Tag[k] != v {
			return false
		}
	}
	return true
}
