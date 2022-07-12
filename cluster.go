package lrmr

import (
	"fmt"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/coordinator"
	"github.com/creasty/defaults"
)

// Cluster represents a lrmr cluster.
type Cluster struct {
	cluster.Cluster

	ClusterOptions cluster.Options

	coordinator coordinator.Coordinator
}

// WithCoordinator sets coordinator for cluster.
func WithCoordinator(crd coordinator.Coordinator) func(*Cluster) {
	return func(c *Cluster) {
		c.coordinator = crd
	}
}

// WithClusterOptions sets cluster options.
func WithClusterOptions(options cluster.Options) func(*Cluster) {
	return func(c *Cluster) {
		c.ClusterOptions = options
	}
}

// Close closes coordinator after closing cluster.
func (c *Cluster) Close() error {
	if err := c.Cluster.Close(); err != nil {
		return err
	}
	return c.coordinator.Close()
}

func newDefaultCoordinator() (coordinator.Coordinator, error) {
	crd, err := coordinator.NewEtcd([]string{`"127.0.0.1:2379"`}, `lrmr/`, coordinator.EtcdOptions{})
	if err != nil {
		return nil, fmt.Errorf("connect coordinator: %w", err)
	}

	return crd, nil
}

func defaultClusterOptions() (o cluster.Options) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}

	return
}
