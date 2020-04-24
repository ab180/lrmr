package master

import (
	"github.com/creasty/defaults"
	"github.com/therne/lrmr/node"
	"github.com/therne/lrmr/output"
)

type Options struct {
	ListenHost     string `default:"localhost:7600"`
	AdvertisedHost string `default:"localhost:7600"`

	CollectQueueSize int `default:"1000"`

	RPC    node.ManagerOptions
	Output output.Options
}

func DefaultOptions() (o Options) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	return
}
