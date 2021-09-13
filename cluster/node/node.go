package node

import (
	"runtime"

	"github.com/ab180/lrmr/coordinator"
)

type Node struct {
	Host      string `json:"host"`
	Executors int    `json:"executors"`

	// Tag is used for affinity rules (e.g. resource locality, ...)
	Tag map[string]string `json:"tag,omitempty"`
}

func New(host string) *Node {
	return &Node{
		Host:      host,
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

// State represents an ephemeral state of the node.
// It will be cleared automatically after the node stops.
type State coordinator.KV

// Registration represents a registered node to the cluster.
type Registration interface {
	// Info returns a node's information.
	Info() *Node

	// States returns an ephemeral node state.
	States() State

	// Unregister removes node from the cluster's node list, and clears all NodeState.
	Unregister()
}
