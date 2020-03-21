package worker

import (
	"github.com/pkg/errors"
	"github.com/therne/lrmr/lrdd"
	"github.com/therne/lrmr/output"
	"github.com/therne/lrmr/stage"
	"sync"
)

type executors struct {
	executors map[string]*executor
	lock      sync.RWMutex

	stage  stage.Stage
	task   *runningTask
	shards *output.Shards
}

func newExecutors(st stage.Stage, t *runningTask, shards *output.Shards) *executors {
	return &executors{
		executors: make(map[string]*executor),
		stage:     st,
		task:      t,
		shards:    shards,
	}
}

func (ee *executors) Apply(rows []*lrdd.Row) error {
	for _, data := range rows {
		ee.lock.RLock()
		executor, exists := ee.executors[data.Key]
		ee.lock.RUnlock()

		if !exists {
			ee.lock.Lock()
			defer ee.lock.Unlock()

			// set up new executor bound to the key as new partition found
			exec, err := newExecutor(data.Key, ee.stage, ee.task, ee.shards)
			if err != nil {
				return errors.Wrap(err, "new executor")
			}
			if err := exec.Setup(); err != nil {
				return errors.Wrap(err, "setup stage")
			}
			ee.executors[data.Key] = exec
			executor = exec
		}
		if err := executor.Apply([]*lrdd.Row{data}); err != nil {
			return err
		}
	}
	return nil
}

func (ee *executors) Teardown() error {
	for partitionKey, executor := range ee.executors {
		if err := executor.Teardown(); err != nil {
			return errors.Wrapf(err, "teardown %s executor", partitionKey)
		}
	}
	return nil
}

// executor is bound to partition.
type executor struct {
	c      stage.Context
	runner stage.Runner
	output output.Writer
}

func newExecutor(partitionKey string, st stage.Stage, t *runningTask, shards *output.Shards) (*executor, error) {
	var serialized []byte
	if s, ok := t.broadcasts["__stage/"+st.Name].([]byte); ok {
		serialized = s
	}
	runner, err := st.Deserialize(serialized)
	if err != nil {
		return nil, errors.Wrap(err, "deserialize stage")
	}
	// each executor owns its output writer while sharing output connections (=shards)
	out := output.NewStreamWriter(shards)

	return &executor{
		c:      t.createContext(partitionKey),
		runner: runner,
		output: out,
	}, nil
}

func (e *executor) Setup() error {
	return e.runner.Setup(e.c)
}

func (e *executor) Apply(data []*lrdd.Row) error {
	return e.runner.Apply(e.c, data, e.output)
}

func (e *executor) Teardown() error {
	if err := e.runner.Teardown(e.c, e.output); err != nil {
		return err
	}
	return e.output.FlushAndClose()
}
