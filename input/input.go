package input

import "github.com/ab180/lrmr/job"

type Input interface {
	CloseWithStatus(s job.Status) error
}
