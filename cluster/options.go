package cluster

import (
	"time"

	"github.com/creasty/defaults"
)

type Options struct {
	ConnectTimeout time.Duration `default:"3s"`

	// MaxMessageSize specifies the maximum message size in bytes the gRPC client can receive/send.
	// The default value is 500mb.
	MaxMessageSize int `default:"524288000"`

	// LivenessProbeInterval specifies interval for notifying this node's liveness to other nodes.
	// If a liveness probe fails, the node would not be visible until the next tick of the liveness probe.
	LivenessProbeInterval time.Duration `default:"10s"`

	TLSCertPath       string
	TLSCertServerName string
}

func DefaultOptions() (o Options) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	return
}

type ListOption struct {
	Tag map[string]string
}
