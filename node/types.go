package node

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const (
	workloadRefKeyTmpl       = "workloads/node=%s/%s"
	workloadRefKeyPrefixTmpl = "workloads/node=%s"
	workloadRefTmpl          = "%s/%s"
)

var (
	ErrInvalidWorkloadRef = errors.New("invalid workload reference format")
)

type Node struct {
	ID   string `json:"id"`
	Host string `json:"host"`

	// Tag is used for affinity rules (e.g. resource locality, ...)
	Tag map[string]string `json:"tag"`

	// ResourceUtilization is filled by Manager.calculateResourceUtilization
	ResourceUtilization uint64 `json:"-"`
}

func New(host string) *Node {
	return &Node{
		ID:   mustGenerateID(),
		Host: host,
	}
}

func mustGenerateID() string {
	bytes := make([]byte, 8)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}

// WorkloadRef holds information for releasing workload.
type WorkloadRef struct {
	JobID  string
	NodeID string
}

// ParseWorkloadRef parses string representation of workload reference.
func ParseWorkloadRef(raw string) (WorkloadRef, error) {
	idSegs := strings.Split(raw, "/")
	if len(idSegs) != 2 {
		return WorkloadRef{}, ErrInvalidWorkloadRef
	}
	return WorkloadRef{
		JobID:  idSegs[0],
		NodeID: idSegs[1],
	}, nil

}

// EtcdKey returns a key used for storing the workload reference information to etcd.
func (ref WorkloadRef) EtcdKey() string {
	return fmt.Sprintf(workloadRefKeyTmpl, ref.NodeID, ref.JobID)
}

// String returns workload reference in "{{ .ID }}/{{ .NodeID }}" format.
func (ref WorkloadRef) String() string {
	return fmt.Sprintf(workloadRefTmpl, ref.JobID, ref.NodeID)
}
