package node

import (
	"crypto/rand"
	"encoding/hex"
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
		ID:   mustGenerateID("N"),
		Host: host,
	}
}

func mustGenerateID(prefix string) string {
	bytes := make([]byte, 4)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return prefix + hex.EncodeToString(bytes)
}
