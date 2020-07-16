package node

import (
	"context"
	"fmt"
	"path"
	"sync"

	"github.com/airbloc/logger"
	"github.com/coreos/etcd/clientv3"
	"github.com/pkg/errors"
	"github.com/therne/lrmr/coordinator"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
)

var log = logger.New("lrmr.node")

const (
	nodeNs = "nodes"
)

type Manager interface {
	// RegisterSelf registers node to the coordinator and makes it discoverable.
	RegisterSelf(ctx context.Context, n *Node) error

	// UnregisterSelf removes node from the coordinator and deletes its state.
	UnregisterSelf() error

	// NodeStates returns an key-value store interface for saving node state.
	// The state will be deleted automatically after node unregisters or dies.
	NodeStates() coordinator.KV

	// Self returns current node information. Returns nil if the node is not registered yet.
	Self() *Node

	// Connect tries to connect the host and returns gRPC connection.
	// The connection can be pooled and cached, and only one connection per host is maintained.
	Connect(ctx context.Context, host string) (*grpc.ClientConn, error)

	// List discovers node from the coordinator.
	List(context.Context, Type) ([]*Node, error)

	Close() error
}

type manager struct {
	crd  coordinator.Coordinator
	self *Node

	// gRPC options for inter-node communication
	grpcOpts []grpc.DialOption
	conns    sync.Map

	// livenessLease is kept alive until the node is alive.
	// if node dies, the lease will be expired and the keys linked with it will be also deleted.
	livenessLease clientv3.LeaseID
	ctx           context.Context
	cancel        context.CancelFunc

	opt ManagerOptions
}

func NewManager(crd coordinator.Coordinator, opt ManagerOptions) (Manager, error) {
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
	grpcOpts = append(grpcOpts, grpc.WithBlock())

	ctx, cancel := context.WithCancel(context.Background())
	return &manager{
		ctx:      ctx,
		cancel:   cancel,
		crd:      crd,
		grpcOpts: grpcOpts,
		opt:      opt,
	}, nil
}

// RegisterSelf is used for registering information of this node to etcd.
func (m *manager) RegisterSelf(ctx context.Context, n *Node) error {
	lease, err := m.crd.GrantLease(ctx, m.opt.LivenessProbeInterval)
	if err != nil {
		return errors.Wrap(err, "grant TTL")
	}
	if err := m.crd.KeepAlive(m.ctx, lease); err != nil {
		return errors.Wrap(err, "start liveness prove")
	}
	m.livenessLease = lease
	if err := m.crd.Put(ctx, path.Join(nodeNs, n.ID), n, coordinator.WithLease(m.livenessLease)); err != nil {
		return errors.Wrap(err, "register node info")
	}
	m.self = n
	log.Info("{type} node {id} registered with", logger.Attrs{"type": n.Type, "id": n.ID, "host": n.Host})
	return nil
}

// NodeStates returns an key-value store interface for saving node state.
// The state will be deleted automatically after node unregisters or dies.
func (m *manager) NodeStates() coordinator.KV {
	return m.crd.WithOptions(coordinator.WithLease(m.livenessLease))
}

// Self returns a information of this node.
func (m *manager) Self() *Node {
	return m.self
}

func (m *manager) Connect(ctx context.Context, host string) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, m.opt.ConnectTimeout)
	defer cancel()

	conn, ok := m.conns.Load(host)
	if !ok {
		return m.establishNewConnection(dialCtx, host)
	}
	c := conn.(*grpc.ClientConn)
	if c.GetState() == connectivity.TransientFailure {
		// TODO: retry limit
		m.conns.Delete(host)
		return m.establishNewConnection(dialCtx, host)
	}
	return c, nil
}

func (m *manager) establishNewConnection(ctx context.Context, host string) (*grpc.ClientConn, error) {
	c, err := grpc.DialContext(ctx, host, m.grpcOpts...)
	if err != nil {
		return nil, err
	}
	m.conns.Store(host, c)
	return c, nil
}

func (m *manager) List(ctx context.Context, typ Type) (nn []*Node, err error) {
	items, err := m.crd.Scan(ctx, nodeNs)
	if err != nil {
		return nil, errors.Wrap(err, "scan etcd")
	}
	for _, item := range items {
		n := new(Node)
		if err := item.Unmarshal(n); err != nil {
			return nil, errors.Wrapf(err, "unmarshal item %s", item.Key)
		}
		if n.Type != typ {
			continue
		}
		nn = append(nn, n)
	}
	return
}

func (m *manager) UnregisterSelf() error {
	if _, err := m.crd.Delete(context.Background(), path.Join(nodeNs, m.self.ID)); err != nil {
		return fmt.Errorf("failed to remove from etcd: %v", err)
	}
	m.self = nil
	m.cancel()
	return nil
}

func (m *manager) Close() (err error) {
	if m.self != nil {
		m.self = nil
		m.cancel()
	}
	m.conns.Range(func(k, v interface{}) bool {
		err = v.(*grpc.ClientConn).Close()
		return err == nil
	})
	return err
}
