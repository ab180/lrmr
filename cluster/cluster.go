package cluster

import (
	"context"
	"path"
	"sync"
	"time"

	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/pkg/retry"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

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
	defaultCallOptions := []grpc.CallOption{
		grpc.MaxCallRecvMsgSize(opt.MaxMessageSize),
		grpc.MaxCallSendMsgSize(opt.MaxMessageSize),
	}
	if opt.Compressor != "" {
		defaultCallOptions = append(defaultCallOptions, grpc.UseCompressor(opt.Compressor))
	}

	grpcOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithDefaultCallOptions(defaultCallOptions...),
	}
	if opt.TLSCertPath != "" {
		cert, err := credentials.NewClientTLSFromFile(opt.TLSCertPath, opt.TLSCertServerName)
		if err != nil {
			return nil, errors.Wrapf(err, "load TLS cert in %s", opt.TLSCertPath)
		}
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(cert))
	} else {
		// log.Warn("inter-node RPC is in insecure mode. we recommend configuring TLS credentials.")
		grpcOpts = append(grpcOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
	return newNodeRegistration(ctx, n, c, c.options.LivenessProbeInterval)
}

// Connect tries to connect the host and returns gRPC connection.
// The connection can be pooled and cached, and only one connection per host is maintained.
func (c *cluster) Connect(ctx context.Context, host string) (*grpc.ClientConn, error) {
	c.grpcConnsMu.Lock()
	conn, ok := c.grpcConns[host]
	c.grpcConnsMu.Unlock()

	if ok && conn.GetState() == connectivity.Ready {
		return conn, nil
	}

	return c.establishNewConnection(ctx, host)
}

// establishNewConnection creates a new connection to given host. the context is only used for
// dialing the host, and cancelling the context after the method return does not affect the connection.
//
// this method is not race-protected; you need to acquire lock before calling the method.
func (c *cluster) establishNewConnection(ctx context.Context, host string) (*grpc.ClientConn, error) {
	log.Info().
		Str("host", host).
		Msg("establish new connection")

	return retry.DoWithResult(
		func() (*grpc.ClientConn, error) {
			dialCtx, cancel := context.WithTimeout(ctx, c.options.ConnectTimeout)
			defer cancel()

			newConn, err := grpc.DialContext(
				dialCtx,
				host,
				c.grpcOptions...,
			)
			if err != nil {
				return nil, err
			}

			c.grpcConnsMu.Lock()
			defer c.grpcConnsMu.Unlock()

			oldConn, ok := c.grpcConns[host]
			if ok && oldConn.GetState() == connectivity.Ready {
				err := newConn.Close()
				if err != nil {
					log.Warn().
						Err(err).
						Str("host", host).
						Msg("failed to close connection")
				}

				return oldConn, nil
			}

			c.grpcConns[host] = newConn
			return newConn, nil
		},
		retry.WithRetryCount(c.options.ConnectRetryCount),
		retry.WithDelay(c.options.ConnectRetryDelay),
	)
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
	mu                    sync.RWMutex
	ctx                   context.Context
	cancel                context.CancelFunc
	node                  *node.Node
	cluster               Cluster
	livenessProbeInterval time.Duration
	livenessLease         clientv3.LeaseID
	sig                   <-chan struct{}
}

func newNodeRegistration(ctx context.Context, n *node.Node, c *cluster, ttl time.Duration) (*nodeRegistration, error) {
	nodeCtx, cancel := context.WithCancel(c.ctx)
	nodeReg := &nodeRegistration{
		ctx:                   nodeCtx,
		cancel:                cancel,
		node:                  n,
		cluster:               c,
		livenessProbeInterval: c.options.LivenessProbeInterval,
	}

	lease, err := c.clusterState.GrantLease(ctx, c.options.LivenessProbeInterval)
	if err != nil {
		return nil, errors.Wrap(err, "grant TTL")
	}
	nodeReg.livenessLease = lease

	sig, err := c.clusterState.KeepAlive(nodeCtx, lease)
	if err != nil {
		return nil, errors.Wrap(err, "start liveness prove")
	}
	nodeReg.sig = sig

	if err := nodeReg.States().Put(ctx, path.Join(nodeNs, n.Host), n); err != nil {
		return nil, errors.Wrap(err, "register node info")
	}

	log.Info().
		Str("host", n.Host).
		Interface("tag", n.Tag).
		Msg("executor node registered")

	go nodeReg.keepRegistered()

	return nodeReg, nil
}

func (n *nodeRegistration) keepRegistered() {
	for n.ctx.Err() == nil {
		select {
		case <-n.ctx.Done():
			return
		case <-n.sig:
		}

		timeoutCtx, cancel := context.WithTimeout(n.ctx, n.livenessProbeInterval)
		func() {
			n.mu.Lock()
			defer n.mu.Unlock()

			log.Info().
				Str("host", n.node.Host).
				Interface("tag", n.node.Tag).
				Msg("re-register node")

			lease, err := n.cluster.States().GrantLease(timeoutCtx, n.livenessProbeInterval)
			if err != nil {
				log.Warn().
					Err(err).
					Msg("failed to get lease TTL")
				return
			}
			n.livenessLease = lease

			sig, err := n.cluster.States().KeepAlive(n.ctx, n.livenessLease)
			if err != nil {
				log.Warn().
					Err(err).
					Msg("failed to keep alive")
				return
			}
			n.sig = sig

			err = n.states().Put(timeoutCtx, path.Join(nodeNs, n.node.Host), n.node)
			if err != nil {
				log.Warn().
					Err(err).
					Msg("failed to put node")
				return
			}
		}()
		cancel()
	}
}

// Info returns a node's information.
func (n *nodeRegistration) Info() *node.Node {
	return n.node
}

func (n *nodeRegistration) states() node.State {
	return n.cluster.States().WithOptions(coordinator.WithLease(n.livenessLease))
}

// States returns an NodeState, which is ephemeral.
func (n *nodeRegistration) States() node.State {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.states()
}

// Unregister removes node from the cluster's node list, and clears all NodeState.
func (n *nodeRegistration) Unregister() {
	n.cancel()
}
