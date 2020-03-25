package input

import "github.com/therne/lrmr/job"

type Input interface {
	CloseWithStatus(s job.Status) error
}
