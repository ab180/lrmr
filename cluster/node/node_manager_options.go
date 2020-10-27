package node

import (
	"time"

	"github.com/creasty/defaults"
)

type ManagerOptions struct {
	ConnectTimeout time.Duration `default:"3s"`

	// LivenessProbeInterval specifies interval for notifying this node's liveness to other nodes.
	// If a liveness probe fails, the node would not be visible until the next tick of the liveness probe.
	LivenessProbeInterval time.Duration `default:"10s"`

	TLSCertPath       string
	TLSCertServerName string
}

func DefaultManagerOptions() (o ManagerOptions) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	return
}
