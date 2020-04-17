package node

import (
	"github.com/creasty/defaults"
	"time"
)

type ManagerOptions struct {
	ConnectTimeout time.Duration `default:"3s"`

	TLSCertPath       string
	TLSCertServerName string
}

func DefaultManagerOptions() (o ManagerOptions) {
	if err := defaults.Set(&o); err != nil {
		panic(err)
	}
	return
}
