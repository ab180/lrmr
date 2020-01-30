package node

import "time"

type ManagerOptions struct {
	ConnectTimeout time.Duration

	WorkloadTimeToLive time.Duration

	TLSCertPath       string
	TLSCertServerName string
}

func DefaultManagerOptions() *ManagerOptions {
	return &ManagerOptions{
		ConnectTimeout:     3 * time.Second,
		WorkloadTimeToLive: 15 * time.Second,
	}
}
