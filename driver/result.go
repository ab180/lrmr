package driver

import (
	"context"
	"sync"

	"github.com/ab180/lrmr/job"
	"github.com/ab180/lrmr/lrdd"
	lrmrmetric "github.com/ab180/lrmr/metric"
	"github.com/hashicorp/go-multierror"
)

// Result is the interface that result of a driver run.
//
// Outputs returns a channel that will yield rows.
// Metrics returns the metrics for the job.
// Err returns the error that occurred during the run.
// Cancel cancels the run.
type Result interface {
	Outputs() <-chan *lrdd.Row
	Metrics() (lrmrmetric.Metrics, error)
	Err() error
	Cancel()
}

type result struct {
	rowChan    chan *lrdd.Row
	err        *multierror.Error
	jobManager job.Manager
	mux        sync.Mutex
	cancel     context.CancelFunc
}

// Outputs returns the output row channel of the job.
func (r *result) Outputs() <-chan *lrdd.Row {
	return r.rowChan
}

// Metrics returns the metrics of the job.
func (r *result) Metrics() (lrmrmetric.Metrics, error) {
	return r.jobManager.CollectMetrics(context.Background())
}

// Err returns the error of the job.
func (r *result) Err() error {
	// Flush remaining rows.
	for _ = range r.rowChan { //nolint:gosimple
	}

	if r.err.ErrorOrNil() == nil {
		return nil
	}

	newErr := &multierror.Error{
		Errors: make([]error, len(r.err.Errors)),
	}
	copy(newErr.Errors, r.err.Errors)

	return newErr
}

// Cancel cancels the job.
func (r *result) Cancel() {
	r.cancel()
}

func (r *result) addErr(err error) {
	r.mux.Lock()
	defer r.mux.Unlock()

	r.err = multierror.Append(r.err, err)
}
