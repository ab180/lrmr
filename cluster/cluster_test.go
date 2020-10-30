package cluster

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/therne/lrmr/cluster/node"
	"github.com/therne/lrmr/test/integration"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const (
	tick        = 300 * time.Millisecond
	testTimeout = 10 * tick
	numNodes    = 3
)

func TestCluster_List(t *testing.T) {
	Convey("Given a cluster", t, WithCluster(func(ctx context.Context, cluster Cluster) {
		Convey("Calling List()", WithTestNodes(cluster, func(nodes []node.Registration) {
			Convey("should return a list of discovered nodes", func() {
				listedNodes, err := cluster.List(ctx)
				So(err, ShouldBeNil)
				So(listedNodes, ShouldHaveLength, len(nodes))
			})

			Convey("With filter, should return nodes with specific type", func() {
				listedNodes, err := cluster.List(ctx, ListOption{Type: node.Worker})
				So(err, ShouldBeNil)
				So(listedNodes, ShouldHaveLength, len(nodes)-1)
				for _, n := range listedNodes {
					So(n.Type, ShouldEqual, node.Worker)
				}
			})

			Convey("With selector, should match nodes by a tag", func() {
				listedNodes, err := cluster.List(ctx, ListOption{Tag: map[string]string{"No": "2"}})
				So(err, ShouldBeNil)
				So(listedNodes, ShouldHaveLength, 1)
				So(listedNodes[0].Host, ShouldEqual, nodes[2].Info().Host)
			})
		}))
	}))
}

func TestCluster_Register(t *testing.T) {
	Convey("Given a cluster", t, WithCluster(func(ctx context.Context, cluster Cluster) {
		Convey("Node information should be registered", func() {
			_, err := cluster.Register(ctx, &node.Node{
				Host: "test",
				Type: node.Worker,
			})
			So(err, ShouldBeNil)
		})

		Convey("Registered node information should be removed after unregister", func() {
			nr, err := cluster.Register(ctx, &node.Node{
				Host: "test",
				Type: node.Worker,
			})
			So(err, ShouldBeNil)

			nr.Unregister()
			time.Sleep(tick)

			_, err = cluster.Get(ctx, "test")
			So(err, ShouldBeError, ErrNotFound)
		})
	}))
}

func TestCluster_Connect(t *testing.T) {
	Convey("Given a cluster", t, WithCluster(func(ctx context.Context, cluster Cluster) {
		Convey("With connectable nodes", WithTestNodes(cluster, func(nodes []node.Registration) {
			Convey("It should be connected without error", func() {
				for i := 0; i < numNodes; i++ {
					cli, err := cluster.Connect(ctx, nodes[i].Info().Host)
					So(err, ShouldBeNil)
					So(cli.GetState(), ShouldEqual, connectivity.Ready)
				}
			})

			Convey("Connection should be maintained and cached", func() {
				for i := 0; i < numNodes; i++ {
					initial, err := cluster.Connect(ctx, nodes[i].Info().Host)
					So(err, ShouldBeNil)

					after, err := cluster.Connect(ctx, nodes[i].Info().Host)
					So(err, ShouldBeNil)

					So(initial, ShouldEqual, after)
				}
			})
		}))
	}))
}

func WithCluster(fn func(context.Context, Cluster)) func(c C) {
	return func(c C) {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)

		opt := DefaultOptions()
		opt.LivenessProbeInterval = tick
		cluster, err := OpenRemote(integration.ProvideEtcd(), opt)
		So(err, ShouldBeNil)

		Reset(func() {
			err = cluster.Close()
			So(err, ShouldBeNil)
			cancel()
		})

		fn(ctx, cluster)
	}
}

func WithTestNodes(cluster Cluster, fn func(nodes []node.Registration)) func(c C) {
	return func(c C) {
		servers := make([]*grpc.Server, numNodes)
		nodes := make([]node.Registration, numNodes)

		for i := 0; i < numNodes; i++ {
			lis, err := net.Listen("tcp", "127.0.0.1:")
			So(err, ShouldBeNil)

			servers[i] = grpc.NewServer()
			go func(i int) {
				err := servers[i].Serve(lis)
				c.So(err, ShouldBeNil)
			}(i)

			n := &node.Node{
				Host: lis.Addr().String(),
				Type: node.Worker,
				Tag: map[string]string{
					"No": strconv.Itoa(i),
				},
			}
			if i == 0 {
				n.Type = node.Master
			}
			nodes[i], err = cluster.Register(context.TODO(), n)
			So(err, ShouldBeNil)
		}
		fn(nodes)

		Reset(func() {
			for i := 0; i < numNodes; i++ {
				servers[i].Stop()
				nodes[i].Unregister()
			}
		})
	}
}
