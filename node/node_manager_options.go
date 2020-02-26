package node

import "time"

type ManagerOptions struct {
	ConnectTimeout time.Duration

	TLSCertPath       string
	TLSCertServerName string
}

func DefaultManagerOptions() *ManagerOptions {
	return &ManagerOptions{
		ConnectTimeout: 3 * time.Second,
	}
}
