package cluster_test

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/cluster/node"
	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/internal/errgroup"
	"github.com/ab180/lrmr/test/integration"
	"github.com/ab180/lrmr/test/testutils"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const (
	tick        = time.Second
	testTimeout = 10 * tick
	numNodes    = 3
)

func TestCluster_List(t *testing.T) {
	Convey("Given a cluster", t, WithCluster(func(ctx context.Context, c cluster.Cluster) {
		Convey("Calling List()", WithTestNodes(t, c, func(nodes []node.Registration) {
			Convey("should return a list of discovered nodes", func() {
				listedNodes, err := c.List(ctx)
				So(err, ShouldBeNil)
				So(listedNodes, ShouldHaveLength, len(nodes))
			})

			Convey("With selector, should match nodes by a tag", func() {
				listedNodes, err := c.List(ctx, cluster.ListOption{Tag: map[string]string{"No": "2"}})
				So(err, ShouldBeNil)
				So(listedNodes, ShouldHaveLength, 1)
				So(listedNodes[0].Host, ShouldEqual, nodes[2].Info().Host)
			})
		}))
	}))
}

func TestCluster_Register(t *testing.T) {
	Convey("Given a cluster", t, WithCluster(func(ctx context.Context, c cluster.Cluster) {
		Convey("Node information should be registered", func() {
			_, err := c.Register(ctx, &node.Node{
				Host: "test",
			})
			So(err, ShouldBeNil)
		})

		Convey("Registered node information should be removed after unregister", func() {
			nr, err := c.Register(ctx, &node.Node{
				Host: "test",
			})
			So(err, ShouldBeNil)

			nr.Unregister()
			time.Sleep(tick)

			_, err = c.Get(ctx, "test")
			So(err, ShouldBeError, cluster.ErrNotFound)
		})
	}))
}

func TestCluster_Connect(t *testing.T) {
	Convey("Given a cluster", t, WithCluster(func(ctx context.Context, c cluster.Cluster) {
		Convey("With connectable nodes", WithTestNodes(t, c, func(nodes []node.Registration) {
			Convey("It should be connected without error", func() {
				for i := 0; i < numNodes; i++ {
					cli, err := c.Connect(ctx, nodes[i].Info().Host)
					So(err, ShouldBeNil)
					So(cli.GetState(), ShouldEqual, connectivity.Ready)
				}
			})

			Convey("Connection should be maintained and cached", func() {
				for i := 0; i < numNodes; i++ {
					initial, err := c.Connect(ctx, nodes[i].Info().Host)
					So(err, ShouldBeNil)

					after, err := c.Connect(ctx, nodes[i].Info().Host)
					So(err, ShouldBeNil)

					So(initial, ShouldEqual, after)
				}
			})

			Convey("Should not leak when connecting in a race condition", func() {
				var wg errgroup.Group
				for i := 0; i < 10; i++ {
					wg.Go(func() error {
						_, err := c.Connect(ctx, nodes[0].Info().Host)
						return err
					})
				}
				So(wg.Wait(), ShouldBeNil)
				// leak is detected within WithCluster HoF
			})

			Convey("Should be reconnected automatically", func() {
				for i := 0; i < numNodes; i++ {
					failingCtx, cancelFailingCtx := context.WithCancel(testutils.ContextWithTimeout())
					initial, err := c.Connect(failingCtx, nodes[i].Info().Host)
					So(err, ShouldBeNil)
					cancelFailingCtx()

					_ = initial.Close()
					time.Sleep(100 * time.Millisecond)

					after, err := c.Connect(ctx, nodes[i].Info().Host)
					So(err, ShouldBeNil)

					So(initial, ShouldNotEqual, after)
				}
			})
		}))
	}))
}

func TestCluster_NodeStates(t *testing.T) {
	Convey("Given a cluster", t, WithCluster(func(ctx context.Context, c cluster.Cluster) {
		nodeReg, err := c.Register(ctx, &node.Node{
			Host: "test",
		})
		if err != nil {
			So(err, ShouldBeNil)
		}

		Convey("Node states should be removed after unregister", func() {
			err := nodeReg.States().Put(ctx, "hello", "world")
			So(err, ShouldBeNil)

			var world string
			err = nodeReg.States().Get(ctx, "hello", &world)
			So(err, ShouldBeNil)
			So(world, ShouldEqual, "world")

			nodeReg.Unregister()
			time.Sleep(4 * tick)

			err = nodeReg.States().Get(ctx, "hello", &world)
			So(err, ShouldBeError, coordinator.ErrNotFound)
		})
	}))
}

func WithCluster(fn func(context.Context, cluster.Cluster)) func() {
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		etcd, closeEtcd := integration.ProvideEtcd()

		opt := cluster.DefaultOptions()
		opt.LivenessProbeInterval = tick
		c, err := cluster.OpenRemote(etcd, opt)
		So(err, ShouldBeNil)

		Reset(func() {
			err = c.Close()
			So(err, ShouldBeNil)
			cancel()
			closeEtcd()
			So(goleak.Find(), ShouldBeNil)
		})

		fn(ctx, c)
	}
}

func WithTestNodes(t *testing.T, cluster cluster.Cluster, fn func(nodes []node.Registration)) func(c C) {
	return func(c C) {
		servers := make([]*grpc.Server, numNodes)
		nodes := make([]node.Registration, numNodes)

		for i := 0; i < numNodes; i++ {
			lis, err := net.Listen("tcp", "127.0.0.1:")
			require.Nil(t, err)

			servers[i] = grpc.NewServer()
			go func(i int) {
				err := servers[i].Serve(lis)
				if err != nil {
					panic(err)
				}
			}(i)

			n := &node.Node{
				Host: lis.Addr().String(),
				Tag: map[string]string{
					"No": strconv.Itoa(i),
				},
			}
			nodes[i], err = cluster.Register(context.TODO(), n)
			require.Nil(t, err)
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
