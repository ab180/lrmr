package cluster

import (
	"context"
	"path"
	"sync"

	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/coordinator"
	"github.com/airbloc/logger"
	"github.com/pkg/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var log = logger.New("lrmr.cluster")

const nodeNs = "nodes"

// ErrNotFound is returned when an node with given host is not found.
var ErrNotFound = errors.New("node not found")

// State is cluster-wide state in coordinator.
// It is ensured to be permanent and consistent in distributed environment.
type State coordinator.Coordinator

type Cluster interface {
	// Register registers node to the coordinator and makes it discoverable.
	// registration will be automatically deleted if cluster's context is cancelled.
	Register(context.Context, *node.Node) (node.Registration, error)

	// Connect tries to connect the host and returns gRPC connection.
	// The connection can be pooled and cached, and only one connection per host is maintained.
	Connect(ctx context.Context, host string) (*grpc.ClientConn, error)

	// List returns a list of available nodes.
	List(context.Context, ...ListOption) ([]*node.Node, error)

	// Get returns an information of node with the host.
	// It returns ErrNotFound if node with given host does not exist.
	Get(ctx context.Context, host string) (*node.Node, error)

	// States returns a cluster-wide state.
	States() State

	// Close unregisters registered nodes and closes all connections.
	Close() error
}

type cluster struct {
	ctx    context.Context
	cancel context.CancelFunc

	clusterState State
	grpcOptions  []grpc.DialOption
	grpcConns    map[string]*grpc.ClientConn
	grpcConnsMu  sync.Mutex
	options      Options
}

func OpenRemote(clusterState coordinator.Coordinator, opt Options) (Cluster, error) {
	grpcOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(opt.MaxMessageSize),
			grpc.MaxCallSendMsgSize(opt.MaxMessageSize),
		),
	}
	if opt.TLSCertPath != "" {
		cert, err := credentials.NewClientTLSFromFile(opt.TLSCertPath, opt.TLSCertServerName)
		if err != nil {
			return nil, errors.Wrapf(err, "load TLS cert in %s", opt.TLSCertPath)
		}
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(cert))
	} else {
		// log.Warn("inter-node RPC is in insecure mode. we recommend configuring TLS credentials.")
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &cluster{
		ctx:          ctx,
		cancel:       cancel,
		grpcOptions:  grpcOpts,
		grpcConns:    make(map[string]*grpc.ClientConn),
		clusterState: clusterState,
		options:      opt,
	}, nil
}

// Register registers node to the coordinator and makes it discoverable.
// registration will be automatically deleted if cluster's context is cancelled.
func (c *cluster) Register(ctx context.Context, n *node.Node) (node.Registration, error) {
	nodeCtx, cancel := context.WithCancel(c.ctx)
	nodeReg := &nodeRegistration{
		ctx:     nodeCtx,
		cancel:  cancel,
		cluster: c,
		node:    n,
	}

	lease, err := c.clusterState.GrantLease(ctx, c.options.LivenessProbeInterval)
	if err != nil {
		return nil, errors.Wrap(err, "grant TTL")
	}
	if err := c.clusterState.KeepAlive(nodeCtx, lease); err != nil {
		return nil, errors.Wrap(err, "start liveness prove")
	}
	nodeReg.livenessLease = lease
	if err := nodeReg.States().Put(ctx, path.Join(nodeNs, n.Host), n); err != nil {
		return nil, errors.Wrap(err, "register node info")
	}
	log.Info("executor node registered as {} (Tag: {})", n.Host, n.Tag)
	return nodeReg, nil
}

// Connect tries to connect the host and returns gRPC connection.
// The connection can be pooled and cached, and only one connection per host is maintained.
func (c *cluster) Connect(ctx context.Context, host string) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, c.options.ConnectTimeout)
	defer cancel()

	c.grpcConnsMu.Lock()
	defer c.grpcConnsMu.Unlock()

	conn, ok := c.grpcConns[host]
	if !ok {
		return c.establishNewConnection(dialCtx, host)
	}
	if conn.GetState() != connectivity.Ready {
		log.Verbose("Connection to {} was on {}. reconnecting...", host, conn.GetState())
		// TODO: retry limit
		delete(c.grpcConns, host)
		return c.establishNewConnection(dialCtx, host)
	}
	return conn, nil
}

// establishNewConnection creates a new connection to given host. the context is only used for
// dialing the host, and cancelling the context after the method return does not affect the connection.
//
// this method is not race-protected; you need to acquire lock before calling the method.
func (c *cluster) establishNewConnection(ctx context.Context, host string) (*grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, host, c.grpcOptions...)
	if err != nil {
		return nil, err
	}
	c.grpcConns[host] = conn
	return conn, nil
}

// List returns a list of available nodes.
func (c *cluster) List(ctx context.Context, option ...ListOption) ([]*node.Node, error) {
	var opt ListOption
	if len(option) > 0 {
		opt = option[0]
	}
	items, err := c.clusterState.Scan(ctx, nodeNs)
	if err != nil {
		return nil, errors.Wrap(err, "scan etcd")
	}

	var nodes []*node.Node
	for _, item := range items {
		n := new(node.Node)
		if err := item.Unmarshal(n); err != nil {
			return nil, errors.Wrapf(err, "unmarshal item %s", item.Key)
		}
		if opt.Tag != nil && !n.TagMatches(opt.Tag) {
			continue
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// Get returns an information of node with the host.
// It returns cluster.ErrNotFound if node with given host does not exist.
func (c *cluster) Get(ctx context.Context, host string) (*node.Node, error) {
	n := new(node.Node)
	if err := c.clusterState.Get(ctx, path.Join(nodeNs, host), n); err != nil {
		if err == coordinator.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, errors.Wrapf(err, "get etcd")
	}
	return n, nil
}

// States returns a cluster-wide state.
func (c *cluster) States() State {
	return c.clusterState
}

// Close unregisters registered nodes and closes all connections.
func (c *cluster) Close() (err error) {
	c.cancel()
	c.grpcConnsMu.Lock()
	defer c.grpcConnsMu.Unlock()

	for host, conn := range c.grpcConns {
		if closeErr := conn.Close(); err == nil {
			if status.Code(closeErr) == codes.Canceled {
				continue
			}
			err = errors.Wrapf(closeErr, "close connection to %s", host)
		}
	}
	return err
}

// nodeRegistration implements node.Registration.
type nodeRegistration struct {
	ctx           context.Context
	cancel        context.CancelFunc
	cluster       Cluster
	node          *node.Node
	livenessLease clientv3.LeaseID
}

// Info returns a node's information.
func (n nodeRegistration) Info() *node.Node {
	return n.node
}

// States returns an NodeState, which is ephemeral.
func (n nodeRegistration) States() node.State {
	return n.cluster.States().WithOptions(coordinator.WithLease(n.livenessLease))
}

// Unregister removes node from the cluster's node list, and clears all NodeState.
func (n nodeRegistration) Unregister() {
	n.cancel()
}
