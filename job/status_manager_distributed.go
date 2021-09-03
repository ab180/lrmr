package job

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/ab180/lrmr/cluster"
	"github.com/ab180/lrmr/coordinator"
	"github.com/pkg/errors"
)

const (
	jobStatusFmt      = "status/jobs/%s"
	stageStatusFmt    = "status/jobs/%s/stages/%s"
	jobErrorNs        = "errors/jobs"
	doneTasksSuffix   = "doneTasks"
	failedTasksSuffix = "failedTasks"
	doneStagesSuffix  = "doneStages"
)

type DistributedStatusManager struct {
	clusterState       cluster.State
	job                *Job
	jobSubscriptions   []func(*Status)
	stageSubscriptions []func(stageName string, stageStatus *StageStatus)
	taskSubscriptions  []func(stageName string, doneCountInStage int)
}

func NewDistributedStatusManager(clusterState cluster.State, job *Job) StatusManager {
	r := &DistributedStatusManager{
		clusterState: clusterState,
		job:          job,
	}
	go r.watch()
	return r
}

func (r *DistributedStatusManager) MarkTaskAsSucceed(ctx context.Context, taskID TaskID) error {
	doneTasks, err := r.clusterState.IncrementCounter(ctx, doneTasksInStageKey(taskID))
	if err != nil {
		return errors.Wrap(err, "write etcd")
	}
	return r.checkForStageCompletion(ctx, taskID, int(doneTasks), 0)
}

// MarkTaskAsFailed marks the task as failed. If the error is non-nil, it's added to the error list of the job.
// Passing nil in error will only cancel the task.
func (r *DistributedStatusManager) MarkTaskAsFailed(ctx context.Context, taskID TaskID, err error) error {
	txn := coordinator.NewTxn().
		IncrementCounter(doneTasksInStageKey(taskID)).
		IncrementCounter(failedTasksInStageKey(taskID))

	if err != nil {
		errDesc := Error{
			Task:       taskID.String(),
			Message:    err.Error(),
			Stacktrace: fmt.Sprintf("%+v", err),
		}
		txn = txn.Put(jobErrorKey(taskID), errDesc)
	}
	if _, err := r.clusterState.Commit(ctx, txn); err != nil {
		return errors.Wrap(err, "write etcd")
	}
	return r.reportJobCompletion(ctx, Failed)
}

func (r *DistributedStatusManager) checkForStageCompletion(ctx context.Context, taskID TaskID, currentDoneTasks, currentFailedTasks int) error {
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

func (r *DistributedStatusManager) reportStageCompletion(ctx context.Context, stageName string, status RunningState) error {
	log.Verbose("Reporting {} stage {}", status, stageName)

	s := newStageStatus()
	s.Complete(status)
	if err := r.clusterState.Put(ctx, stageStatusKey(r.job.ID, stageName), s); err != nil {
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

func (r *DistributedStatusManager) reportJobCompletion(ctx context.Context, status RunningState) error {
	// var js Status
	// if err := r.clusterState.Get(ctx, jobStatusKey(r.job.ID), &js); err != nil {
	// 	return errors.Wrapf(err, "get status of job %s", r.job.ID)
	// }
	// if js.Status == status {
	// 	return nil
	// }
	js := newStatus()

	log.Verbose("Reporting job {}", status)
	js.Complete(status)
	if err := r.clusterState.Put(ctx, jobStatusKey(r.job.ID), js); err != nil {
		return errors.Wrapf(err, "update status of job %s", r.job.ID)
	}
	return nil
}

func (r *DistributedStatusManager) Abort(ctx context.Context, abortedBy TaskID) error {
	// var js Status
	// if err := r.clusterState.Get(ctx, jobStatusKey(r.job.ID), &js); err != nil {
	// 	return errors.Wrapf(err, "get status of job %s", r.job.ID)
	// }
	// if js.Status == Failed {
	// 	return nil
	// }
	js := newStatus()
	js.Complete(Failed)

	errDesc := Error{
		Task:    abortedBy.String(),
		Message: "aborted",
	}
	txn := coordinator.NewTxn().
		Put(jobErrorKey(abortedBy), errDesc).
		Put(jobStatusKey(r.job.ID), js)

	if _, err := r.clusterState.Commit(ctx, txn); err != nil {
		return errors.Wrap(err, "write etcd")
	}
	return nil
}

// OnJobCompletion registers callback for completion events of given job.
func (r *DistributedStatusManager) OnJobCompletion(callback func(*Status)) {
	r.jobSubscriptions = append(r.jobSubscriptions, callback)
}

// OnStageCompletion registers callback for stage completion events in given job ID.
func (r *DistributedStatusManager) OnStageCompletion(callback func(stageName string, stageStatus *StageStatus)) {
	r.stageSubscriptions = append(r.stageSubscriptions, callback)
}

// OnTaskCompletion registers callback for task completion events in given job ID.
// For performance, only the number of currently finished tasks in its stage is given to the callback.
func (r *DistributedStatusManager) OnTaskCompletion(callback func(stageName string, doneCountInStage int)) {
	r.taskSubscriptions = append(r.taskSubscriptions, callback)
}

func (r *DistributedStatusManager) watch() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	defer log.Recover()

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

func (r *DistributedStatusManager) handleTaskFinish(e coordinator.WatchEvent) {
	stageName, err := stageNameFromStageStatusKey(e.Item.Key)
	if err != nil {
		log.Warn("Unable to handle task finish event: {}", err)
		return
	}
	for _, callback := range r.taskSubscriptions {
		go callback(stageName, int(e.Counter))
	}
}

func (r *DistributedStatusManager) handleStageStatusUpdate(e coordinator.WatchEvent) {
	stageName, err := stageNameFromStageStatusKey(e.Item.Key)
	if err != nil {
		log.Warn("Unable to handle stage status update event: {}", err)
		return
	}

	st := new(StageStatus)
	if err := e.Item.Unmarshal(st); err != nil {
		log.Error("Failed to unmarshal stage status on {}", err, e.Item.Key)
		return
	}
	for _, callback := range r.stageSubscriptions {
		go callback(stageName, st)
	}
}

func (r *DistributedStatusManager) handleJobStatusChange() (finished bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	jobStatus, err := r.getJobStatus(ctx, r.job.ID)
	if err != nil {
		log.Error("Failed to get job status: {}", err)
		return false
	}
	if jobStatus.Status == Succeeded || jobStatus.Status == Failed {
		for _, callback := range r.jobSubscriptions {
			go callback(jobStatus)
		}
		return true
	}
	return false
}

func (r *DistributedStatusManager) getJobStatus(ctx context.Context, jobID string) (*Status, error) {
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

func (r *DistributedStatusManager) getJobErrors(ctx context.Context, jobID string) ([]Error, error) {
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
