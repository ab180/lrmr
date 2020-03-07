package node

import (
	"github.com/therne/lrmr/internal/utils"
)

type Node struct {
	ID   string `json:"id"`
	Host string `json:"host"`

	// Tag is used for affinity rules (e.g. resource locality, ...)
	Tag map[string]string `json:"tag,omitempty"`

	// ResourceUtilization is filled by Manager.calculateResourceUtilization
	ResourceUtilization uint64 `json:"-"`
}

func New(host string) *Node {
	return &Node{
		ID:   utils.GenerateID("N"),
		Host: host,
	}
}
