package job

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/coordinator"
	"github.com/ab180/lrmr/internal/util"
	lrmrmetric "github.com/ab180/lrmr/metric"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
)

const (
	// jobLogRetention specifies the lifespan of job state data on coordinator.
	jobLogRetention = 24 * time.Hour

	jobStatusFmt      = "status/jobs/%s"
	stageStatusFmt    = "status/jobs/%s/stages/%s"
	jobErrorNs        = "errors/jobs"
	jobMetricsNs      = "metrics/jobs"
	taskMetricsFmt    = "metrics/jobs/%s/%s/%s"
	doneTasksSuffix   = "doneTasks"
	failedTasksSuffix = "failedTasks"
	doneStagesSuffix  = "doneStages"
)

type DistributedManager struct {
	clusterState       cluster.State
	job                *Job
	jobSubscriptions   []func(*Status)
	stageSubscriptions []func(stageName string, stageStatus *StageStatus)
	taskSubscriptions  []func(stageName string, doneCountInStage int)
	mu                 sync.RWMutex
	logRetentionLease  clientv3.LeaseID
	cancelWatch        context.CancelFunc
	closed             atomic.Bool
	id                 string
}

func NewDistributedManager(clusterState cluster.State, job *Job) Manager {
	watchCtx, cancelWatch := context.WithCancel(context.Background())
	r := &DistributedManager{
		clusterState: clusterState,
		job:          job,
		cancelWatch:  cancelWatch,
		id:           util.GenerateID("DM"),
	}
	go r.watch(watchCtx)
	return r
}

func (r *DistributedManager) getLeaseForLogRetention(ctx context.Context) (clientv3.LeaseID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.logRetentionLease != clientv3.NoLease {
		return r.logRetentionLease, nil
	}
	l, err := r.clusterState.GrantLease(ctx, jobLogRetention)
	if err != nil {
		return clientv3.NoLease, err
	}
	return l, nil
}

func (r *DistributedManager) RegisterStatus(ctx context.Context) (*Status, error) {
	l, err := r.getLeaseForLogRetention(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "grant lease for job log retention")
	}

	js := newStatus()
	if err := r.clusterState.Put(ctx, jobStatusKey(r.job.ID), js, coordinator.WithLease(l)); err != nil {
		return nil, errors.Wrap(err, "write status to etcd")
	}
	return js, nil
}

func (r *DistributedManager) FetchStatus(ctx context.Context) (*Status, error) {
	js, err := r.getJobStatus(ctx, r.job.ID)
	if err != nil {
		if err == coordinator.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, errors.Wrapf(err, "get status of job %s", r.job.ID)
	}
	return js, nil
}

func (r *DistributedManager) MarkTaskAsSucceed(ctx context.Context, taskID TaskID, metrics lrmrmetric.Metrics) error {
	l, err := r.getLeaseForLogRetention(ctx)
	if err != nil {
		return errors.Wrap(err, "grant lease for job log retention")
	}
	if err := r.clusterState.Put(ctx, taskMetricsKey(taskID), metrics, coordinator.WithLease(l)); err != nil {
		return errors.Wrap(err, "write task metrics to etcd")
	}

	doneTasks, err := r.clusterState.IncrementCounter(ctx, doneTasksInStageKey(taskID))
	if err != nil {
		return errors.Wrap(err, "write etcd")
	}
	return r.checkForStageCompletion(ctx, taskID, int(doneTasks), 0)
}

// MarkTaskAsFailed marks the task as failed. If the error is non-nil, it's added to the error list of the job.
// Passing nil in error will only cancel the task.
func (r *DistributedManager) MarkTaskAsFailed(ctx context.Context, taskID TaskID, taskErr error, metrics lrmrmetric.Metrics) error { //nolint:lll
	l, err := r.getLeaseForLogRetention(ctx)
	if err != nil {
		return errors.Wrap(err, "grant lease for job log retention")
	}

	txn := coordinator.NewTxn().
		Put(taskMetricsKey(taskID), metrics, clientv3.WithLease(l)).
		IncrementCounter(doneTasksInStageKey(taskID), clientv3.WithLease(l)).
		IncrementCounter(failedTasksInStageKey(taskID), clientv3.WithLease(l))

	if taskErr != nil {
		errDesc := Error{
			Task:       taskID.String(),
			Message:    taskErr.Error(),
			Stacktrace: fmt.Sprintf("%+v", err),
		}
		txn = txn.Put(jobErrorKey(taskID), errDesc, clientv3.WithLease(l))
	}
	if _, err := r.clusterState.Commit(ctx, txn); err != nil {
		return errors.Wrap(err, "write etcd")
	}
	return r.reportJobCompletion(ctx, Failed)
}

func (r *DistributedManager) checkForStageCompletion(ctx context.Context, taskID TaskID, currentDoneTasks, currentFailedTasks int) error { //nolint:lll
	if currentFailedTasks == 1 {
		// to prevent race between workers, the failure is only reported by the first worker failed
		if err := r.reportStageCompletion(ctx, taskID.StageName, Failed); err != nil {
			return errors.Wrap(err, "report failure of stage")
		}
		return nil
	}
	totalTasks := len(r.job.GetPartitionsOfStage(taskID.StageName))
	if currentDoneTasks == totalTasks {
		failedTasks, err := r.clusterState.ReadCounter(ctx, failedTasksInStageKey(taskID))
		if err != nil {
			return errors.Wrap(err, "read count of failed stage")
		}
		if failedTasks > 0 {
			return nil
		}
		if err := r.reportStageCompletion(ctx, taskID.StageName, Succeeded); err != nil {
			return errors.Wrap(err, "report success of stage")
		}
	}
	return nil
}

func (r *DistributedManager) reportStageCompletion(ctx context.Context, stageName string, status RunningState) error {
	s := newStageStatus()
	s.Complete(status)
	if err := r.clusterState.Put(ctx, stageStatusKey(r.job.ID, stageName), s, coordinator.WithLease(r.logRetentionLease)); err != nil { //nolint:lll
		return errors.Wrap(err, "update stage status")
	}
	if status == Failed {
		return r.reportJobCompletion(ctx, Failed)
	}

	doneStages, err := r.clusterState.IncrementCounter(ctx, doneStagesKey(r.job.ID))
	if err != nil {
		return errors.Wrap(err, "increment done stage count")
	}
	totalStages := int64(len(r.job.Stages)) - 1
	if doneStages == totalStages {
		return r.reportJobCompletion(ctx, Succeeded)
	}
	return nil
}

func (r *DistributedManager) reportJobCompletion(ctx context.Context, status RunningState) error {
	js, err := r.FetchStatus(ctx)
	if err != nil {
		return err
	}
	if js.Status == status {
		return nil
	}

	log.Debug().
		Str("job_id", r.job.ID).
		Str("status", string(status)).
		Msg("reporting job")
	js.Complete(status)
	if err := r.clusterState.Put(ctx, jobStatusKey(r.job.ID), js, coordinator.WithLease(r.logRetentionLease)); err != nil {
		return errors.Wrapf(err, "update status of job %s", r.job.ID)
	}
	return nil
}

func (r *DistributedManager) Abort(ctx context.Context, abortedBy TaskID) error {
	l, err := r.getLeaseForLogRetention(ctx)
	if err != nil {
		return errors.Wrap(err, "grant lease for job log retention")
	}

	js, err := r.FetchStatus(ctx)
	if err != nil {
		return err
	}
	if js.Status == Failed {
		return nil
	}
	js.Complete(Failed)

	errDesc := Error{
		Task:    abortedBy.String(),
		Message: "aborted",
	}
	txn := coordinator.NewTxn().
		Put(jobErrorKey(abortedBy), errDesc, clientv3.WithLease(l)).
		Put(jobStatusKey(r.job.ID), js, clientv3.WithLease(l))

	if _, err := r.clusterState.Commit(ctx, txn); err != nil {
		return errors.Wrap(err, "write etcd")
	}
	return nil
}

// OnJobCompletion registers callback for completion events of given job.
func (r *DistributedManager) OnJobCompletion(callback func(*Status)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.jobSubscriptions = append(r.jobSubscriptions, callback)
}

// OnStageCompletion registers callback for stage completion events in given job ID.
func (r *DistributedManager) OnStageCompletion(callback func(stageName string, stageStatus *StageStatus)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.stageSubscriptions = append(r.stageSubscriptions, callback)
}

// OnTaskCompletion registers callback for task completion events in given job ID.
// For performance, only the number of currently finished tasks in its stage is given to the callback.
func (r *DistributedManager) OnTaskCompletion(callback func(stageName string, doneCountInStage int)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.taskSubscriptions = append(r.taskSubscriptions, callback)
}

func (r *DistributedManager) watch(ctx context.Context) {
	defer func() {
		r := recover()
		if r != nil {
			log.Warn().
				Interface("panic", r).
				Msg("panic in watch")
		}
	}()
	defer r.Close()

	prefix := jobStatusKey(r.job.ID)
	for event := range r.clusterState.Watch(ctx, prefix) {
		if strings.HasPrefix(event.Item.Key, jobStatusKey(r.job.ID, "stage")) {
			if strings.HasSuffix(event.Item.Key, doneTasksSuffix) {
				r.handleTaskFinish(event)

			} else if strings.HasSuffix(event.Item.Key, failedTasksSuffix) {
				// TODO: handle failed tasks

			} else {
				r.handleStageStatusUpdate(event)
			}
		} else if event.Item.Key == jobStatusKey(r.job.ID) {
			finished := r.handleJobStatusChange()
			if finished {
				return
			}
		}
	}
}

func (r *DistributedManager) handleTaskFinish(e coordinator.WatchEvent) {
	stageName, err := stageNameFromStageStatusKey(e.Item.Key)
	if err != nil {
		log.Warn().
			Err(err).
			Msg("unable to handle task finish event")
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, callback := range r.taskSubscriptions {
		go callback(stageName, int(e.Counter))
	}
}

func (r *DistributedManager) handleStageStatusUpdate(e coordinator.WatchEvent) {
	stageName, err := stageNameFromStageStatusKey(e.Item.Key)
	if err != nil {
		log.Warn().
			Err(err).
			Msg("unable to handle stage status update event")
		return
	}

	st := new(StageStatus)
	if err := e.Item.Unmarshal(st); err != nil {
		log.Warn().
			Err(err).
			Str("key", e.Item.Key).
			Msg("unable to unmarshal stage status")
		return
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, callback := range r.stageSubscriptions {
		go callback(stageName, st)
	}
}

func (r *DistributedManager) handleJobStatusChange() (finished bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	jobStatus, err := r.getJobStatus(ctx, r.job.ID)
	if err != nil {
		log.Warn().
			Err(err).
			Msg("failed to get job status")
		return false
	}
	if jobStatus.Status == Succeeded || jobStatus.Status == Failed {
		r.mu.RLock()
		defer r.mu.RUnlock()

		for _, callback := range r.jobSubscriptions {
			go callback(jobStatus)
		}
		return true
	}
	return false
}

func (r *DistributedManager) getJobStatus(ctx context.Context, jobID string) (*Status, error) {
	s := new(Status)
	if err := r.clusterState.Get(ctx, jobStatusKey(jobID), s); err != nil {
		return nil, err
	}
	errs, err := r.getJobErrors(ctx, r.job.ID)
	if err != nil {
		return nil, errors.Wrapf(err, "list errors of job %s", jobID)
	}
	s.Errors = errs
	return s, nil
}

func (r *DistributedManager) getJobErrors(ctx context.Context, jobID string) ([]Error, error) {
	items, err := r.clusterState.Scan(ctx, path.Join(jobErrorNs, jobID))
	if err != nil {
		return nil, err
	}
	errs := make([]Error, len(items))
	for i, item := range items {
		if err := item.Unmarshal(&errs[i]); err != nil {
			return nil, errors.Wrapf(err, "unmarshal item %s", item.Key)
		}
	}
	return errs, nil
}

func (r *DistributedManager) CollectMetrics(ctx context.Context) (lrmrmetric.Metrics, error) {
	items, err := r.clusterState.Scan(ctx, path.Join(jobMetricsNs, r.job.ID))
	if err != nil {
		return nil, err
	}
	metrics := make(lrmrmetric.Metrics)
	for _, item := range items {
		var metric lrmrmetric.Metrics
		if err := item.Unmarshal(&metric); err != nil {
			return nil, errors.Wrapf(err, "unmarshal item %s", item.Key)
		}
		metrics.Add(metric)
	}
	return metrics, nil
}

func (r *DistributedManager) Close() {
	if swapped := r.closed.CAS(false, true); !swapped {
		return
	}
	r.cancelWatch()
}

func jobStatusKey(jobID string, extra ...string) string {
	base := fmt.Sprintf(jobStatusFmt, jobID)
	return path.Join(append([]string{base}, extra...)...)
}

// stageStatusKey returns a key of stage summary entry with given name.
func stageStatusKey(jobID, stageName string, extra ...string) string {
	base := fmt.Sprintf(stageStatusFmt, jobID, stageName)
	return path.Join(append([]string{base}, extra...)...)
}

func stageNameFromStageStatusKey(stageStatusKey string) (string, error) {
	pathFrags := strings.Split(stageStatusKey, "/")
	if len(pathFrags) < 5 {
		return "", errors.Errorf("invalid stage status key format: %s", stageStatusKey)
	}
	return pathFrags[4], nil
}

// jobErrorKey returns a key of job error entry with given name.
func jobErrorKey(ref TaskID) string {
	return path.Join(jobErrorNs, ref.String())
}

func doneTasksInStageKey(t TaskID) string {
	return stageStatusKey(t.JobID, t.StageName, doneTasksSuffix)
}

func failedTasksInStageKey(t TaskID) string {
	return stageStatusKey(t.JobID, t.StageName, failedTasksSuffix)
}

func doneStagesKey(jobID string) string {
	return jobStatusKey(jobID, doneStagesSuffix)
}

func taskMetricsKey(task TaskID) string {
	return path.Join(
		fmt.Sprintf(taskMetricsFmt, task.JobID, task.StageName, task.PartitionID),
		"/metrics",
	)
}
