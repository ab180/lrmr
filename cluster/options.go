package cluster

import (
	"time"

	_ "github.com/ab180/lrmr/pkg/encoding/lz4"
	"github.com/creasty/defaults"
	_ "google.golang.org/grpc/encoding/gzip"
)

type Options struct {
	ConnectTimeout time.Duration `default:"3s"`

	// ConnectRetryCount specifies the maximum number of retries to connect to other nodes.
	ConnectRetryCount int `default:"10"`

	// ConnectRetryDelay specifies the delay between retries to connect to other nodes.
	ConnectRetryDelay time.Duration `default:"200ms"`

	// MaxMessageSize specifies the maximum message size in bytes the gRPC client can receive/send.
	// The default value is 500mb.
	MaxMessageSize int `default:"524288000"`

	// LivenessProbeInterval specifies interval for notifying this node's liveness to other nodes.
	// If a liveness probe fails, the node would not be visible until the next tick of the liveness probe.
	LivenessProbeInterval time.Duration `default:"10s"`

	TLSCertPath       string
	TLSCertServerName string

	// Compressor specifies the compressor to use for messages.
	Compressor string `default:""`
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
