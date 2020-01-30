package node

import (
	"context"
	"errors"
	"fmt"
	"github.com/airbloc/logger"
	"github.com/json-iterator/go"
	"github.com/therne/lrmr/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"path"
)

var (
	// ErrNodeNotFound is raised when node information does not exist with given node ID.
	ErrNodeNotFound = errors.New("node not found")

	// ErrNodeIDEmpty is raised when an empty node ID is given.
	ErrNodeIDEmpty = errors.New("node ID is empty")
)

const (
	nodeNs = "nodes"
)

type Manager interface {
	RegisterSelf(ctx context.Context, n *Node) error
	Self() *Node

	Connect(ctx context.Context, n *Node) (*grpc.ClientConn, error)

	GetInfo(ctx context.Context, nid string) (*Node, error)
	List(ctx context.Context) ([]*Node, error)

	UnregisterNode(nid string) error

	JobScheduler
}

type manager struct {
	coordinator coordinator.Coordinator
	self        *Node

	// gRPC options for inter-node communication
	grpcOpts []grpc.DialOption

	opt *ManagerOptions
	log logger.Logger
}

func NewManager(crd coordinator.Coordinator, opt *ManagerOptions) (Manager, error) {
	log := logger.New("nodemanager")

	var grpcOpts []grpc.DialOption
	if opt.TLSCertPath != "" {
		cert, err := credentials.NewClientTLSFromFile(opt.TLSCertPath, opt.TLSCertServerName)
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS cert: %v", err)
		}
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(cert))
	} else {
		log.Warn("inter-node RPC is in insecure mode. we recommend configuring TLS credentials.")
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}

	return &manager{
		coordinator: crd,
		grpcOpts:    grpcOpts,
		log:         log,
		opt:         opt,
	}, nil
}

// RegisterSelf is used for registering information of this node to etcd.
func (m *manager) RegisterSelf(ctx context.Context, n *Node) error {
	val, err := jsoniter.MarshalToString(n)
	if err != nil {
		return err
	}
	key := path.Join(nodeNs, n.ID)
	if err := m.coordinator.Put(ctx, key, val); err != nil {
		return fmt.Errorf("failed to write to etcd: %v", err)
	}
	m.self = n
	m.log.Info("node {id} registered with", logger.Attrs{"id": n.ID, "host": n.Host})
	return nil
}

// Self returns a information of this node.
func (m *manager) Self() *Node {
	return m.self
}

func (m *manager) Connect(ctx context.Context, n *Node) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, n.Host, m.grpcOpts...)
}

func (m *manager) GetInfo(ctx context.Context, nid string) (*Node, error) {
	if len(nid) == 0 {
		return nil, ErrNodeIDEmpty
	}
	if nid == m.self.ID {
		return m.self, nil
	}

	var n Node
	if err := m.coordinator.Get(ctx, path.Join(nodeNs, nid), &n); err != nil {
		if err == coordinator.ErrNotFound {
			return nil, ErrNodeNotFound
		}
		return nil, fmt.Errorf("get node info: %v", err)
	}
	resourceUtilization, err := m.calculateResourceUtilization(ctx, nid)
	if err != nil {
		return nil, fmt.Errorf("unable to calculate workload: %v", err)
	}
	n.ResourceUtilization = resourceUtilization
	return &n, nil
}

func (m *manager) List(ctx context.Context) ([]*Node, error) {
	items, err := m.coordinator.Scan(ctx, nodeNs)
	if err != nil {
		return nil, fmt.Errorf("failed to read from etcd: %v", err)
	}

	nn := make([]*Node, len(items))
	for _, item := range items {
		var n Node
		if err := item.Unmarshal(&n); err != nil {
			return nil, fmt.Errorf("unable to unmarshal one of the node info: %v", err)
		}
		n.ResourceUtilization, err = m.calculateResourceUtilization(ctx, n.ID)
		if err != nil {
			return nil, fmt.Errorf("unable to calculate workload: %v", err)
		}
		nn = append(nn, &n)
	}
	return nn, nil
}

func (m *manager) UnregisterNode(nid string) error {
	key := path.Join(nodeNs, nid)
	if _, err := m.coordinator.Delete(context.Background(), key); err != nil {
		return fmt.Errorf("failed to remove from etcd: %v", err)
	}
	return nil
}
